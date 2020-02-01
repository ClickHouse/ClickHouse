SELECT
toTypeName(ui64), toTypeName(i64),
toTypeName(ui32), toTypeName(i32),
toTypeName(ui16), toTypeName(i16),
toTypeName(ui8), toTypeName(i8)
FROM generate('ui64 UInt64, i64 Int64, ui32 UInt32, i32 Int32, ui16 UInt16, i16 Int16, ui8 UInt8, i8 Int8', 1);
SELECT
ui64, i64,
ui32, i32,
ui16, i16,
ui8, i8
FROM generate('ui64 UInt64, i64 Int64, ui32 UInt32, i32 Int32, ui16 UInt16, i16 Int16, ui8 UInt8, i8 Int8', 10);
SELECT
toTypeName(d), toTypeName(dt), toTypeName(dtm)
FROM generate('d Date, dt DateTime, dtm DateTime(\'Europe/Moscow\')', 1);
SELECT
d, dt, dtm
FROM generate('d Date, dt DateTime, dtm DateTime(\'Europe/Moscow\')', 10) FORMAT JSONEachRow;;
SELECT
toTypeName(dt64), toTypeName(dts64), toTypeName(dtms64)
FROM generate('dt64 DateTime64, dts64 DateTime64(6), dtms64 DateTime64(6 ,\'Europe/Moscow\')', 1);
SELECT
dt64, dts64, dtms64
FROM generate('dt64 DateTime64, dts64 DateTime64(6), dtms64 DateTime64(6 ,\'Europe/Moscow\')', 10) FORMAT JSONEachRow;
SELECT
dt64, dts64, dtms64
FROM generate('dt64 DateTime64, dts64 DateTime64(6), dtms64 DateTime64(6 ,\'Europe/Moscow\')', 10);
SELECT
  toTypeName(f32), toTypeName(f64)
FROM generate('f32 Float32, f64 Float64', 1);
SELECT
  f32, f64
FROM generate('f32 Float32, f64 Float64', 10) FORMAT JSONEachRow;
SELECT
  toTypeName(d32), toTypeName(d64)
FROM generate('d32 Decimal32(4), d64 Decimal64(8)', 1);
SELECT
  d32, d64
FROM generate('d32 Decimal32(4), d64 Decimal64(8)', 10) FORMAT JSONEachRow;
SELECT
  toTypeName(i)
FROM generate('i Interval', 10);
SELECT
  i
FROM generate('i Interval', 10) FORMAT JSONEachRow;
