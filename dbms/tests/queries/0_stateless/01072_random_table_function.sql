SELECT
  toTypeName(i)
FROM generate('i Enum8(\'hello\' = 1, \'world\' = 5)', 1);
SELECT
  i
FROM generate('i Enum8(\'hello\' = 1, \'world\' = 5)', 10, 10, 10, 1);
SELECT '-';
SELECT
  toTypeName(i)
FROM generate('i Array(Nullable(Enum8(\'hello\' = 1, \'world\' = 5)))', 1);
SELECT
  i
FROM generate('i Array(Nullable(Enum8(\'hello\' = 1, \'world\' = 5)))', 10, 10, 10, 1);
SELECT '-';
SELECT
  toTypeName(i)s
FROM generate('i Nullable(Enum16(\'h\' = 1, \'w\' = 5 , \'o\' = -200)))', 1);
SELECT
  i
FROM generate('i Nullable(Enum16(\'h\' = 1, \'w\' = 5 , \'o\' = -200)))', 10, 10, 10, 1);
SELECT '-';
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
FROM generate('ui64 UInt64, i64 Int64, ui32 UInt32, i32 Int32, ui16 UInt16, i16 Int16, ui8 UInt8, i8 Int8', 10, 10, 10, 1);
SELECT '-';
SELECT
toTypeName(d), toTypeName(dt), toTypeName(dtm)
FROM generate('d Date, dt DateTime, dtm DateTime(\'Europe/Moscow\')', 1);
SELECT
d, dt, dtm
FROM generate('d Date, dt DateTime, dtm DateTime(\'Europe/Moscow\')', 10, 10, 10, 1);
SELECT '-';
SELECT
toTypeName(dt64), toTypeName(dts64), toTypeName(dtms64)
FROM generate('dt64 DateTime64, dts64 DateTime64(6), dtms64 DateTime64(6 ,\'Europe/Moscow\')', 1);
SELECT
dt64, dts64, dtms64
FROM generate('dt64 DateTime64, dts64 DateTime64(6), dtms64 DateTime64(6 ,\'Europe/Moscow\')', 10, 10, 10, 1);
SELECT '-';
SELECT
  toTypeName(f32), toTypeName(f64)
FROM generate('f32 Float32, f64 Float64', 1);
SELECT
  f32, f64
FROM generate('f32 Float32, f64 Float64', 10, 10, 10, 1);
SELECT '-';
SELECT
  toTypeName(d32), toTypeName(d64), toTypeName(d64)
FROM generate('d32 Decimal32(4), d64 Decimal64(8), d128 Decimal128(16)', 1);
SELECT
  d32, d64, d128
FROM generate('d32 Decimal32(4), d64 Decimal64(8), d128 Decimal128(16)', 10, 10, 10, 1);
SELECT '-';
SELECT
  toTypeName(i)
FROM generate('i UUID', 1);
SELECT
  i
FROM generate('i UUID', 10, 10, 10, 1);
SELECT '-';
SELECT
  toTypeName(i)
FROM generate('i Tuple(Int32, Int64)', 1);
SELECT
  i
FROM generate('i Tuple(Int32, Int64)', 10, 10, 10, 1);
SELECT '-';
SELECT
  toTypeName(i)
FROM generate('i Array(Int8)', 1);
SELECT
  i
FROM generate('i Array(Int8)', 10, 10, 10, 1);
SELECT '-';
SELECT
  toTypeName(i)
FROM generate('i Array(Nullable(Int32))', 1);
SELECT
  i
FROM generate('i Array(Nullable(Int32))', 10, 10, 10, 1);
SELECT '-';
SELECT
  toTypeName(i)
FROM generate('i Array(Nullable(UUID))', 1);
SELECT
  i
FROM generate('i Array(Nullable(UUID))', 10, 10, 10, 1);
SELECT '-';
SELECT
  toTypeName(i)
FROM generate('i Tuple(Int32, Array(Int64))', 1);
SELECT
  i
FROM generate('i Tuple(Int32, Array(Int64))', 10, 10, 10, 1);
SELECT '-';
SELECT
   toTypeName(i)
FROM generate('i FixedString(4)', 1);
SELECT
  i
FROM generate('i FixedString(4)', 10, 10, 10, 1);
SELECT '-';
SELECT
  toTypeName(i)
FROM generate('i String', 10);
SELECT
  i
FROM generate('i String', 10, 10, 10, 1);
SELECT '-';
SELECT
  toTypeName(i)
FROM generate('i Nullable(String)', 1);
SELECT
  i
FROM generate('i Nullable(String)', 10, 10, 10, 1);
SELECT '-';
SELECT
  toTypeName(i)
FROM generate('i Array(String)', 1);
SELECT
  i
FROM generate('i Array(String)', 10, 10, 10, 1);

SELECT '-';

DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table(a Array(Int8), d Decimal32(4), c Tuple(DateTime64(3), UUID)) ENGINE=Memory;
INSERT INTO test_table SELECT * FROM generate('a Array(Int8), d Decimal32(4), c Tuple(DateTime64(3), UUID)', 3, 2, 10, 1);

SELECT * FROM test_table;

DROP TABLE IF EXISTS test_table;

SELECT '-';

DROP TABLE IF EXISTS test_table_2;
CREATE TABLE test_table_2(a Array(Int8), b UInt32, c Nullable(String), d Decimal32(4), e Nullable(Enum16('h' = 1, 'w' = 5 , 'o' = -200)), f Float64, g Tuple(Date, DateTime, DateTime64, UUID), h FixedString(2)) ENGINE=Memory;
INSERT INTO test_table_2 SELECT * FROM generate('a Array(Int8), b UInt32, c Nullable(String), d Decimal32(4), e Nullable(Enum16(\'h\' = 1, \'w\' = 5 , \'o\' = -200)), f Float64, g Tuple(Date, DateTime, DateTime64, UUID), h FixedString(2)', 10, 3, 5, 10);

SELECT * FROM test_table_2;
SELECT '-';

DROP TABLE IF EXISTS test_table_2;
