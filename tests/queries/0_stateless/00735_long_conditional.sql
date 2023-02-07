-- Tags: long

SET send_logs_level = 'fatal';

SELECT 'value vs value';

SELECT toInt8(0) AS x, toInt8(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toInt8(0) AS x, toInt16(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toInt8(0) AS x, toInt32(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toInt8(0) AS x, toInt64(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toInt8(0) AS x, toUInt8(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toInt8(0) AS x, toUInt16(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toInt8(0) AS x, toUInt32(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toInt8(0) AS x, toUInt64(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 386 }
SELECT toInt8(0) AS x, toFloat32(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toInt8(0) AS x, toFloat64(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toInt8(0) AS x, toDate(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 43 }
SELECT toInt8(0) AS x, toDateTime(1, 'Asia/Istanbul') AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 386 }
SELECT toInt8(0) AS x, toDecimal32(1, 0) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toInt8(0) AS x, toDecimal64(1, 0) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toInt8(0) AS x, toDecimal128(1, 0) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);

SELECT toInt16(0) AS x, toInt8(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toInt16(0) AS x, toInt16(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toInt16(0) AS x, toInt32(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toInt16(0) AS x, toInt64(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toInt16(0) AS x, toUInt8(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toInt16(0) AS x, toUInt16(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toInt16(0) AS x, toUInt32(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toInt16(0) AS x, toUInt64(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 386 }
SELECT toInt16(0) AS x, toFloat32(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toInt16(0) AS x, toFloat64(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toInt16(0) AS x, toDate(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 43 }
SELECT toInt16(0) AS x, toDateTime(1, 'Asia/Istanbul') AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 386 }
SELECT toInt16(0) AS x, toDecimal32(1, 0) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toInt16(0) AS x, toDecimal64(1, 0) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toInt16(0) AS x, toDecimal128(1, 0) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);

SELECT toInt32(0) AS x, toInt8(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toInt32(0) AS x, toInt16(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toInt32(0) AS x, toInt32(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toInt32(0) AS x, toInt64(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toInt32(0) AS x, toUInt8(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toInt32(0) AS x, toUInt16(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toInt32(0) AS x, toUInt32(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toInt32(0) AS x, toUInt64(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 386 }
SELECT toInt32(0) AS x, toFloat32(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toInt32(0) AS x, toFloat64(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toInt32(0) AS x, toDate(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 43 }
SELECT toInt32(0) AS x, toDateTime(1, 'Asia/Istanbul') AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 386 }
SELECT toInt32(0) AS x, toDecimal32(1, 0) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toInt32(0) AS x, toDecimal64(1, 0) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toInt32(0) AS x, toDecimal128(1, 0) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);

SELECT toInt64(0) AS x, toInt8(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toInt64(0) AS x, toInt16(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toInt64(0) AS x, toInt32(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toInt64(0) AS x, toInt64(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toInt64(0) AS x, toUInt8(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toInt64(0) AS x, toUInt16(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toInt64(0) AS x, toUInt32(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toInt64(0) AS x, toUInt64(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 386 }
SELECT toInt64(0) AS x, toFloat32(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 386 }
SELECT toInt64(0) AS x, toFloat64(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 386 }
SELECT toInt64(0) AS x, toDate(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 43 }
SELECT toInt64(0) AS x, toDateTime(1, 'Asia/Istanbul') AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 386 }
SELECT toInt64(0) AS x, toDecimal32(1, 0) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toInt64(0) AS x, toDecimal64(1, 0) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toInt64(0) AS x, toDecimal128(1, 0) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);

SELECT toUInt8(0) AS x, toInt8(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toUInt8(0) AS x, toInt16(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toUInt8(0) AS x, toInt32(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toUInt8(0) AS x, toInt64(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toUInt8(0) AS x, toUInt8(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toUInt8(0) AS x, toUInt16(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toUInt8(0) AS x, toUInt32(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toUInt8(0) AS x, toUInt64(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toUInt8(0) AS x, toFloat32(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toUInt8(0) AS x, toFloat64(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toUInt8(0) AS x, toDate(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 43 }
SELECT toUInt8(0) AS x, toDateTime(1, 'Asia/Istanbul') AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 386 }
SELECT toUInt8(0) AS x, toDecimal32(1, 0) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toUInt8(0) AS x, toDecimal64(1, 0) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toUInt8(0) AS x, toDecimal128(1, 0) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);

SELECT toUInt16(0) AS x, toInt8(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toUInt16(0) AS x, toInt16(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toUInt16(0) AS x, toInt32(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toUInt16(0) AS x, toInt64(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toUInt16(0) AS x, toUInt8(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toUInt16(0) AS x, toUInt16(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toUInt16(0) AS x, toUInt32(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toUInt16(0) AS x, toUInt64(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toUInt16(0) AS x, toFloat32(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toUInt16(0) AS x, toFloat64(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toUInt16(0) AS x, toDate(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 43 }
SELECT toUInt16(0) AS x, toDateTime(1, 'Asia/Istanbul') AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 386 }
SELECT toUInt16(0) AS x, toDecimal32(1, 0) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toUInt16(0) AS x, toDecimal64(1, 0) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toUInt16(0) AS x, toDecimal128(1, 0) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);

SELECT toUInt32(0) AS x, toInt8(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toUInt32(0) AS x, toInt16(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toUInt32(0) AS x, toInt32(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toUInt32(0) AS x, toInt64(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toUInt32(0) AS x, toUInt8(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toUInt32(0) AS x, toUInt16(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toUInt32(0) AS x, toUInt32(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toUInt32(0) AS x, toUInt64(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toUInt32(0) AS x, toFloat32(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toUInt32(0) AS x, toFloat64(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toUInt32(0) AS x, toDate(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 43 }
SELECT toUInt32(0) AS x, toDateTime(1, 'Asia/Istanbul') AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 386 }
SELECT toUInt32(0) AS x, toDecimal32(1, 0) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toUInt32(0) AS x, toDecimal64(1, 0) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toUInt32(0) AS x, toDecimal128(1, 0) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);

SELECT toUInt64(0) AS x, toInt8(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 386 }
SELECT toUInt64(0) AS x, toInt16(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 386 }
SELECT toUInt64(0) AS x, toInt32(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 386 }
SELECT toUInt64(0) AS x, toInt64(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 386 }
SELECT toUInt64(0) AS x, toUInt8(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toUInt64(0) AS x, toUInt16(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toUInt64(0) AS x, toUInt32(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toUInt64(0) AS x, toUInt64(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toUInt64(0) AS x, toFloat32(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 386 }
SELECT toUInt64(0) AS x, toFloat64(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 386 }
SELECT toUInt64(0) AS x, toDate(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 43 }
SELECT toUInt64(0) AS x, toDateTime(1, 'Asia/Istanbul') AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 386 }
SELECT toUInt64(0) AS x, toDecimal32(1, 0) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toUInt64(0) AS x, toDecimal64(1, 0) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toUInt64(0) AS x, toDecimal128(1, 0) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);

SELECT toDate(0) AS x, toInt8(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 43 }
SELECT toDate(0) AS x, toInt16(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 43 }
SELECT toDate(0) AS x, toInt32(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 43 }
SELECT toDate(0) AS x, toInt64(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 43 }
SELECT toDate(0) AS x, toUInt8(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 43 }
SELECT toDate(0) AS x, toUInt16(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 43 }
SELECT toDate(0) AS x, toUInt32(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 43 }
SELECT toDate(0) AS x, toUInt64(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 43 }
SELECT toDate(0) AS x, toFloat32(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 43 }
SELECT toDate(0) AS x, toFloat64(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 43 }
SELECT toDate(0) AS x, toDate(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toDate('2000-01-01') AS x, toDateTime('2000-01-01 00:00:01', 'Asia/Istanbul') AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toDate(0) AS x, toDecimal32(1, 0) AS y, ((x = 0) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 43 }
SELECT toDate(0) AS x, toDecimal64(1, 0) AS y, ((x = 0) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 43 }
SELECT toDate(0) AS x, toDecimal128(1, 0) AS y, ((x = 0) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 43 }

SELECT toDateTime(0, 'Asia/Istanbul') AS x, toInt8(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 386 }
SELECT toDateTime(0, 'Asia/Istanbul') AS x, toInt16(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 386 }
SELECT toDateTime(0, 'Asia/Istanbul') AS x, toInt32(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 386 }
SELECT toDateTime(0, 'Asia/Istanbul') AS x, toInt64(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 386 }
SELECT toDateTime(0, 'Asia/Istanbul') AS x, toUInt8(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 386 }
SELECT toDateTime(0, 'Asia/Istanbul') AS x, toUInt16(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 386 }
SELECT toDateTime(0, 'Asia/Istanbul') AS x, toUInt32(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 386 }
SELECT toDateTime(0, 'Asia/Istanbul') AS x, toUInt64(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 386 }
SELECT toDateTime(0, 'Asia/Istanbul') AS x, toFloat32(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 386 }
SELECT toDateTime(0, 'Asia/Istanbul') AS x, toFloat64(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 386 }
SELECT toDateTime('2000-01-01 00:00:00', 'Asia/Istanbul') AS x, toDate('2000-01-02') AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toDateTime(0, 'Asia/Istanbul') AS x, toDateTime(1, 'Asia/Istanbul') AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT toDateTime(0, 'Asia/Istanbul') AS x, toDecimal32(1, 0) AS y, ((x = 0) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 386 }
SELECT toDateTime(0, 'Asia/Istanbul') AS x, toDecimal64(1, 0) AS y, ((x = 0) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 386 }
SELECT toDateTime(0, 'Asia/Istanbul') AS x, toDecimal128(1, 0) AS y, ((x = 0) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 386 }

SELECT 'column vs value';

SELECT materialize(toInt8(0)) AS x, toInt8(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toInt8(0)) AS x, toInt16(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toInt8(0)) AS x, toInt32(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toInt8(0)) AS x, toInt64(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toInt8(0)) AS x, toUInt8(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toInt8(0)) AS x, toUInt16(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toInt8(0)) AS x, toUInt32(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toInt8(0)) AS x, toUInt64(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 386 }
SELECT materialize(toInt8(0)) AS x, toFloat32(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toInt8(0)) AS x, toFloat64(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toInt8(0)) AS x, toDate(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 43 }
SELECT materialize(toInt8(0)) AS x, toDateTime(1, 'Asia/Istanbul') AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 386 }
SELECT materialize(toInt8(0)) AS x, toDecimal32(1, 0) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toInt8(0)) AS x, toDecimal64(1, 0) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toInt8(0)) AS x, toDecimal128(1, 0) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);

SELECT materialize(toInt16(0)) AS x, toInt8(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toInt16(0)) AS x, toInt16(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toInt16(0)) AS x, toInt32(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toInt16(0)) AS x, toInt64(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toInt16(0)) AS x, toUInt8(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toInt16(0)) AS x, toUInt16(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toInt16(0)) AS x, toUInt32(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toInt16(0)) AS x, toUInt64(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 386 }
SELECT materialize(toInt16(0)) AS x, toFloat32(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toInt16(0)) AS x, toFloat64(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toInt16(0)) AS x, toDate(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 43 }
SELECT materialize(toInt16(0)) AS x, toDateTime(1, 'Asia/Istanbul') AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 386 }
SELECT materialize(toInt16(0)) AS x, toDecimal32(1, 0) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toInt16(0)) AS x, toDecimal64(1, 0) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toInt16(0)) AS x, toDecimal128(1, 0) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);

SELECT materialize(toInt32(0)) AS x, toInt8(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toInt32(0)) AS x, toInt16(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toInt32(0)) AS x, toInt32(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toInt32(0)) AS x, toInt64(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toInt32(0)) AS x, toUInt8(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toInt32(0)) AS x, toUInt16(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toInt32(0)) AS x, toUInt32(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toInt32(0)) AS x, toUInt64(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 386 }
SELECT materialize(toInt32(0)) AS x, toFloat32(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toInt32(0)) AS x, toFloat64(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toInt32(0)) AS x, toDate(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 43 }
SELECT materialize(toInt32(0)) AS x, toDateTime(1, 'Asia/Istanbul') AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 386 }
SELECT materialize(toInt32(0)) AS x, toDecimal32(1, 0) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toInt32(0)) AS x, toDecimal64(1, 0) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toInt32(0)) AS x, toDecimal128(1, 0) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);

SELECT materialize(toInt64(0)) AS x, toInt8(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toInt64(0)) AS x, toInt16(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toInt64(0)) AS x, toInt32(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toInt64(0)) AS x, toInt64(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toInt64(0)) AS x, toUInt8(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toInt64(0)) AS x, toUInt16(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toInt64(0)) AS x, toUInt32(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toInt64(0)) AS x, toUInt64(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 386 }
SELECT materialize(toInt64(0)) AS x, toFloat32(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 386 }
SELECT materialize(toInt64(0)) AS x, toFloat64(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 386 }
SELECT materialize(toInt64(0)) AS x, toDate(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 43 }
SELECT materialize(toInt64(0)) AS x, toDateTime(1, 'Asia/Istanbul') AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 386 }
SELECT materialize(toInt64(0)) AS x, toDecimal32(1, 0) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toInt64(0)) AS x, toDecimal64(1, 0) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toInt64(0)) AS x, toDecimal128(1, 0) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);

SELECT materialize(toUInt8(0)) AS x, toInt8(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toUInt8(0)) AS x, toInt16(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toUInt8(0)) AS x, toInt32(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toUInt8(0)) AS x, toInt64(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toUInt8(0)) AS x, toUInt8(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toUInt8(0)) AS x, toUInt16(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toUInt8(0)) AS x, toUInt32(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toUInt8(0)) AS x, toUInt64(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toUInt8(0)) AS x, toFloat32(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toUInt8(0)) AS x, toFloat64(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toUInt8(0)) AS x, toDate(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 43 }
SELECT materialize(toUInt8(0)) AS x, toDateTime(1, 'Asia/Istanbul') AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 386 }
SELECT materialize(toUInt8(0)) AS x, toDecimal32(1, 0) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toUInt8(0)) AS x, toDecimal64(1, 0) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toUInt8(0)) AS x, toDecimal128(1, 0) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);

SELECT materialize(toUInt16(0)) AS x, toInt8(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toUInt16(0)) AS x, toInt16(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toUInt16(0)) AS x, toInt32(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toUInt16(0)) AS x, toInt64(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toUInt16(0)) AS x, toUInt8(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toUInt16(0)) AS x, toUInt16(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toUInt16(0)) AS x, toUInt32(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toUInt16(0)) AS x, toUInt64(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toUInt16(0)) AS x, toFloat32(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toUInt16(0)) AS x, toFloat64(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toUInt16(0)) AS x, toDate(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 43 }
SELECT materialize(toUInt16(0)) AS x, toDateTime(1, 'Asia/Istanbul') AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 386 }
SELECT materialize(toUInt16(0)) AS x, toDecimal32(1, 0) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toUInt16(0)) AS x, toDecimal64(1, 0) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toUInt16(0)) AS x, toDecimal128(1, 0) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);

SELECT materialize(toUInt32(0)) AS x, toInt8(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toUInt32(0)) AS x, toInt16(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toUInt32(0)) AS x, toInt32(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toUInt32(0)) AS x, toInt64(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toUInt32(0)) AS x, toUInt8(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toUInt32(0)) AS x, toUInt16(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toUInt32(0)) AS x, toUInt32(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toUInt32(0)) AS x, toUInt64(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toUInt32(0)) AS x, toFloat32(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toUInt32(0)) AS x, toFloat64(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toUInt32(0)) AS x, toDate(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 43 }
SELECT materialize(toUInt32(0)) AS x, toDateTime(1, 'Asia/Istanbul') AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 386 }
SELECT materialize(toUInt32(0)) AS x, toDecimal32(1, 0) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toUInt32(0)) AS x, toDecimal64(1, 0) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toUInt32(0)) AS x, toDecimal128(1, 0) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);

SELECT materialize(toUInt64(0)) AS x, toInt8(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 386 }
SELECT materialize(toUInt64(0)) AS x, toInt16(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 386 }
SELECT materialize(toUInt64(0)) AS x, toInt32(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 386 }
SELECT materialize(toUInt64(0)) AS x, toInt64(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 386 }
SELECT materialize(toUInt64(0)) AS x, toUInt8(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toUInt64(0)) AS x, toUInt16(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toUInt64(0)) AS x, toUInt32(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toUInt64(0)) AS x, toUInt64(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toUInt64(0)) AS x, toFloat32(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 386 }
SELECT materialize(toUInt64(0)) AS x, toFloat64(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 386 }
SELECT materialize(toUInt64(0)) AS x, toDate(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 43 }
SELECT materialize(toUInt64(0)) AS x, toDateTime(1, 'Asia/Istanbul') AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 386 }
SELECT materialize(toUInt64(0)) AS x, toDecimal32(1, 0) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toUInt64(0)) AS x, toDecimal64(1, 0) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toUInt64(0)) AS x, toDecimal128(1, 0) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);

SELECT materialize(toDate(0)) AS x, toInt8(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 43 }
SELECT materialize(toDate(0)) AS x, toInt16(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 43 }
SELECT materialize(toDate(0)) AS x, toInt32(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 43 }
SELECT materialize(toDate(0)) AS x, toInt64(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 43 }
SELECT materialize(toDate(0)) AS x, toUInt8(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 43 }
SELECT materialize(toDate(0)) AS x, toUInt16(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 43 }
SELECT materialize(toDate(0)) AS x, toUInt32(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 43 }
SELECT materialize(toDate(0)) AS x, toUInt64(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 43 }
SELECT materialize(toDate(0)) AS x, toFloat32(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 43 }
SELECT materialize(toDate(0)) AS x, toFloat64(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 43 }
SELECT materialize(toDate(0)) AS x, toDate(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toDate('2000-01-01')) AS x, toDateTime('2000-01-01 00:00:01', 'Asia/Istanbul') AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toDate(0)) AS x, toDecimal32(1, 0) AS y, ((x = 0) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 43 }
SELECT materialize(toDate(0)) AS x, toDecimal64(1, 0) AS y, ((x = 0) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 43 }
SELECT materialize(toDate(0)) AS x, toDecimal128(1, 0) AS y, ((x = 0) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 43 }

SELECT materialize(toDateTime(0, 'Asia/Istanbul')) AS x, toInt8(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 386 }
SELECT materialize(toDateTime(0, 'Asia/Istanbul')) AS x, toInt16(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 386 }
SELECT materialize(toDateTime(0, 'Asia/Istanbul')) AS x, toInt32(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 386 }
SELECT materialize(toDateTime(0, 'Asia/Istanbul')) AS x, toInt64(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 386 }
SELECT materialize(toDateTime(0, 'Asia/Istanbul')) AS x, toUInt8(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 386 }
SELECT materialize(toDateTime(0, 'Asia/Istanbul')) AS x, toUInt16(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 386 }
SELECT materialize(toDateTime(0, 'Asia/Istanbul')) AS x, toUInt32(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 386 }
SELECT materialize(toDateTime(0, 'Asia/Istanbul')) AS x, toUInt64(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 386 }
SELECT materialize(toDateTime(0, 'Asia/Istanbul')) AS x, toFloat32(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 386 }
SELECT materialize(toDateTime(0, 'Asia/Istanbul')) AS x, toFloat64(1) AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 386 }
SELECT materialize(toDateTime('2000-01-01 00:00:00', 'Asia/Istanbul')) AS x, toDate('2000-01-02') AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toDateTime(0, 'Asia/Istanbul')) AS x, toDateTime(1, 'Asia/Istanbul') AS y, ((x > y) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z);
SELECT materialize(toDateTime(0, 'Asia/Istanbul')) AS x, toDecimal32(1, 0) AS y, ((x = 0) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 386 }
SELECT materialize(toDateTime(0, 'Asia/Istanbul')) AS x, toDecimal64(1, 0) AS y, ((x = 0) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 386 }
SELECT materialize(toDateTime(0, 'Asia/Istanbul')) AS x, toDecimal128(1, 0) AS y, ((x = 0) ? x : y) AS z, toTypeName(x), toTypeName(y), toTypeName(z); -- { serverError 386 }
