// client.js
import { io } from 'socket.io-client';
import mysql from 'mysql2/promise';
import dotenv from 'dotenv';
import iconv from 'iconv-lite';
import zlib from 'zlib';
import cron from 'node-cron';

dotenv.config();

/* =========================
   Basics
========================= */
const hosCode = process.env.HOSCODE;
const hosName = process.env.HOSNAME;
const serverUrl = process.env.URL_SERVER;

const socket = io(serverUrl, { transports: ['websocket'] });

/* =========================
   Logging
========================= */
const LOG_PREFIX = `[client ${hosCode}]`;
function logInfo(msg, obj) {
  if (obj !== undefined) console.log(`${LOG_PREFIX} ${msg}`, obj);
  else console.log(`${LOG_PREFIX} ${msg}`);
}
function logWarn(msg, obj) {
  if (obj !== undefined) console.warn(`${LOG_PREFIX} ${msg}`, obj);
  else console.warn(`${LOG_PREFIX} ${msg}`);
}
function logError(msg, err) {
  console.error(`${LOG_PREFIX} ${msg}`, err?.message || err);
}

/* =========================
   Config helpers
========================= */
function getDbConfig(hisType) {
  if (!hisType) throw new Error('hisType ไม่ถูกส่งมา');
  const prefix = hisType.toUpperCase(); // hosxpv3 -> HOSXPV3

  const cfg = {
    host: process.env[`${prefix}_DB_HOST`],
    user: process.env[`${prefix}_DB_USER`],
    password: process.env[`${prefix}_DB_PASSWORD`],
    database: process.env[`${prefix}_DB_NAME`],
    port: Number(process.env[`${prefix}_DB_PORT`]) || 3306,
    charset: 'tis620',
    rowsAsArray: false,
    // ✅ decode TIS-620 ให้เป็น UTF-8
    typeCast(field, next) {
      if (field.type === 'DATE') return field.string();
      if (field.type === 'VAR_STRING' || field.type === 'STRING') {
        const buffer = field.buffer();
        if (buffer) return iconv.decode(buffer, 'tis620');
      }
      return next();
    }
  };

  if (!cfg.host || !cfg.user || !cfg.database) {
    throw new Error(`ไม่พบหรือไม่ครบ DB config สำหรับ hisType=${hisType}`);
  }

  return cfg;
}

/* =========================
   Constants
========================= */
const MAX_ROWS_PER_CHUNK = 5000;       // จำนวนแถวต่อ chunk
const ENABLE_GZIP = true;              // เปิดการบีบอัด gzip
const MAX_RETRY_DELAY_MS = 30000;      // ดีเลย์สูงสุด 30 วินาที
const BASE_RETRY_DELAY_MS = 3000;      // ดีเลย์เริ่มต้น 3 วินาที

/* =========================
   Utility
========================= */
function chunkArray(arr, size) {
  const chunks = [];
  for (let i = 0; i < arr.length; i += size) {
    chunks.push(arr.slice(i, i + size));
  }
  return chunks;
}

function maybeGzipString(str) {
  if (!ENABLE_GZIP) return { compressed: false, payload: str };
  const gz = zlib.gzipSync(Buffer.from(str, 'utf8'));
  return { compressed: true, payload: gz.toString('base64') };
}

/* =========================
   Core fetchData
   - ตอนนี้ client จะรับ SQL มาจาก serverCommand โดยตรง
========================= */
async function fetchData(sqlQuery, queryName, hosCode, params, hisType) {
  let pool;
  let connection;
  const startedAt = Date.now();

  try {
    const dbConfig = getDbConfig(hisType);
    pool = mysql.createPool(dbConfig);
    connection = await pool.getConnection();

    if (!sqlQuery || !sqlQuery.trim()) throw new Error('SQL query ว่างเปล่า');

    logInfo(`SQL [${hisType}/${queryName}]: ${sqlQuery.slice(0, 400)}${sqlQuery.length > 400 ? ' ...' : ''}`);

    const [rows] = await connection.execute(sqlQuery);
    const elapsed = Date.now() - startedAt;

    logInfo(`ดึงข้อมูลสำเร็จ: ${rows.length} แถว (elapsed: ${elapsed}ms)`);

    const enrichedRows = rows.map(row => ({ ...row, hoscode: hosCode }));

    return {
      queryName,
      hisType,
      elapsedMs: elapsed,
      rowCount: enrichedRows.length,
      data: enrichedRows
    };
  } finally {
    if (connection) connection.release();
    if (pool) await pool.end();
  }
}

/* =========================
   Emit data
========================= */
async function emitData({ queryName, hisType, data, silent }) {
  if (!Array.isArray(data) || data.length === 0) {
    logWarn(`ไม่มีข้อมูลสำหรับส่งกลับ (${queryName}/${hisType})`);
    return;
  }

  const chunks = chunkArray(data, MAX_ROWS_PER_CHUNK);
  logInfo(`เตรียมส่งข้อมูลแบบ chunk: ${chunks.length} ชิ้น (${queryName}/${hisType})`);

  for (let i = 0; i < chunks.length; i++) {
    const part = chunks[i];
    const jsonStr = JSON.stringify(part);
    const { compressed, payload } = maybeGzipString(jsonStr);

    socket.emit('clientData', {
      hosCode,
      queryName,
      chunkIndex: i,
      chunkTotal: chunks.length,
      compressed,
      data: payload,
      silent
    });

    logInfo(`ส่ง chunk ${i + 1}/${chunks.length} (rows=${part.length}, gzip=${compressed})`);
  }
}

/* =========================
   Retry wrapper
========================= */
function tryFetchDataWithRetry(sqlQuery, queryName, hosCodeLocal, params, silent, hisType, attempt = 1) {
  if (!socket.connected) {
    logWarn(`Socket ไม่เชื่อม – ยกเลิก fetch สำหรับ ${queryName}/${hosCodeLocal}`);
    return;
  }

  const startedAt = Date.now();
  fetchData(sqlQuery, queryName, hosCodeLocal, params, hisType)
    .then(async (result) => {
      const { data, rowCount } = result;

      if (rowCount === 0) {
        logWarn(`ผลลัพธ์ว่าง: ${queryName}/${hisType}`);
        socket.emit('clientMetric', {
          hosCode,
          queryName,
          hisType,
          status: 'empty',
          elapsedMs: Date.now() - startedAt
        });
        return;
      }

      logInfo(`[Preview] ${queryName}/${hisType} row[0]`, data[0]);

      await emitData({ queryName, hisType, data, silent });

      socket.emit('clientMetric', {
        hosCode,
        queryName,
        hisType,
        status: 'ok',
        rows: rowCount,
        elapsedMs: Date.now() - startedAt
      });
    })
    .catch((err) => {
      logError(`fetchData ล้มเหลว [ครั้งที่ ${attempt}] – ${queryName}/${hisType}`, err);
      socket.emit('clientMetric', {
        hosCode,
        queryName,
        hisType,
        status: 'error',
        message: err.message,
        attempt
      });

      const delay = Math.min(attempt * BASE_RETRY_DELAY_MS, MAX_RETRY_DELAY_MS);
      setTimeout(() => {
        tryFetchDataWithRetry(sqlQuery, queryName, hosCodeLocal, params, silent, hisType, attempt + 1);
      }, delay);
    });
}

/* =========================
   Socket events
========================= */
socket.on('connect', () => {
  logInfo(`เชื่อมต่อกับ server แล้ว: ${serverUrl}`);
  socket.emit('register', { hosCode, hosName });
});

socket.on('greeting', (msg) => {
  logInfo(`Server greeting: ${msg}`);
});

// ✅ ตอนนี้ server จะส่ง SQL มาด้วย
socket.on('serverCommand', ({ queryName, hosCode: hosCodeFromServer, sql, params, hisType, silent }) => {
  logInfo(`รับคำสั่ง: ${queryName} (${hosCodeFromServer}) [hisType=${hisType}]`);
  tryFetchDataWithRetry(sql, queryName, hosCodeFromServer, params, silent, hisType);
});

socket.on('dataResponse', (data) => {
  logInfo('Server dataResponse:', data);
});

socket.on('disconnect', (reason) => {
  logWarn(`Socket disconnect: ${reason}`);
});

/* =========================
   Optional: Client-side cron
   - ใช้เมื่ออยากให้ client ทำงานเองตามเวลา โดยไม่ต้องรอ server trigger
========================= */
const CLIENT_CRON_ENABLED = false; // ตั้ง true เพื่อใช้งาน
const CLIENT_CRON_EXPRESSION = '*/5 * * * *'; // ทุก 5 นาที
const CLIENT_CRON_TASKS = [
  // ตัวอย่าง:
  // { queryName: 'ERMonitorBar', hisType: 'hosxpv3', sql: 'SELECT ...', params: [], silent: true }
];

if (CLIENT_CRON_ENABLED) {
  cron.schedule(CLIENT_CRON_EXPRESSION, () => {
    logInfo(`Client cron tick → ${CLIENT_CRON_EXPRESSION}`);
    for (const t of CLIENT_CRON_TASKS) {
      tryFetchDataWithRetry(t.sql, t.queryName, hosCode, t.params || [], t.silent === true, t.hisType);
    }
  });
}

/* =========================
   Graceful shutdown
========================= */
process.on('SIGINT', () => {
  logInfo('ปิด client แล้ว');
  process.exit(0);
});
