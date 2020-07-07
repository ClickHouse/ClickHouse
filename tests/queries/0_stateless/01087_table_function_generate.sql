SELECT
    toTypeName(ui64), toTypeName(i64),
    toTypeName(ui32), toTypeName(i32),
    toTypeName(ui16), toTypeName(i16),
    toTypeName(ui8), toTypeName(i8)
FROM generateRandom('ui64 UInt64, i64 Int64, ui32 UInt32, i32 Int32, ui16 UInt16, i16 Int16, ui8 UInt8, i8 Int8')
LIMIT 1;
SELECT
    ui64, i64,
    ui32, i32,
    ui16, i16,
    ui8, i8
FROM generateRandom('ui64 UInt64, i64 Int64, ui32 UInt32, i32 Int32, ui16 UInt16, i16 Int16, ui8 UInt8, i8 Int8', 1, 10, 10)
LIMIT 10;
SELECT '-';
SELECT
  toTypeName(i)
FROM generateRandom('i Enum8(\'hello\' = 1, \'world\' = 5)')
LIMIT 1;
SELECT
  i
FROM generateRandom('i Enum8(\'hello\' = 1, \'world\' = 5)', 1, 10, 10)
LIMIT 10;
SELECT '-';
SELECT
  toTypeName(i)
FROM generateRandom('i Array(Nullable(Enum8(\'hello\' = 1, \'world\' = 5)))')
LIMIT 1;
SELECT
  i
FROM generateRandom('i Array(Nullable(Enum8(\'hello\' = 1, \'world\' = 5)))', 1, 10, 10)
LIMIT 10;
SELECT '-';
SELECT
  toTypeName(i)s
FROM generateRandom('i Nullable(Enum16(\'h\' = 1, \'w\' = 5 , \'o\' = -200))')
LIMIT 1;
SELECT
  i
FROM generateRandom('i Nullable(Enum16(\'h\' = 1, \'w\' = 5 , \'o\' = -200))', 1, 10, 10)
LIMIT 10;
SELECT '-';
SELECT
toTypeName(d), toTypeName(dt), toTypeName(dtm)
FROM generateRandom('d Date, dt DateTime, dtm DateTime(\'Europe/Moscow\')')
LIMIT 1;
SELECT
d, dt, dtm
FROM generateRandom('d Date, dt DateTime, dtm DateTime(\'Europe/Moscow\')', 1, 10, 10)
LIMIT 10;
SELECT '-';
SELECT
toTypeName(dt64), toTypeName(dts64), toTypeName(dtms64)
FROM generateRandom('dt64 DateTime64, dts64 DateTime64(6), dtms64 DateTime64(6 ,\'Europe/Moscow\')')
LIMIT 1;
SELECT
dt64, dts64, dtms64
FROM generateRandom('dt64 DateTime64, dts64 DateTime64(6), dtms64 DateTime64(6 ,\'Europe/Moscow\')', 1, 10, 10)
LIMIT 10;
SELECT '-';
SELECT
  toTypeName(f32), toTypeName(f64)
FROM generateRandom('f32 Float32, f64 Float64')
LIMIT 1;
SELECT
  f32, f64
FROM generateRandom('f32 Float32, f64 Float64', 1, 10, 10)
LIMIT 10;
SELECT '-';
SELECT
  toTypeName(d32), toTypeName(d64), toTypeName(d64)
FROM generateRandom('d32 Decimal32(4), d64 Decimal64(8), d128 Decimal128(16)')
LIMIT 1;
SELECT
  d32, d64, d128
FROM generateRandom('d32 Decimal32(4), d64 Decimal64(8), d128 Decimal128(16)', 1, 10, 10)
LIMIT 10;
SELECT '-';
SELECT
  toTypeName(i)
FROM generateRandom('i Tuple(Int32, Int64)')
LIMIT 1;
SELECT
  i
FROM generateRandom('i Tuple(Int32, Int64)', 1, 10, 10)
LIMIT 10;
SELECT '-';
SELECT
  toTypeName(i)
FROM generateRandom('i Array(Int8)')
LIMIT 1;
SELECT
  i
FROM generateRandom('i Array(Int8)', 1, 10, 10)
LIMIT 10;
SELECT '-';
SELECT
  toTypeName(i)
FROM generateRandom('i Array(Nullable(Int32))')
LIMIT 1;
SELECT
  i
FROM generateRandom('i Array(Nullable(Int32))', 1, 10, 10)
LIMIT 10;
SELECT '-';
SELECT
  toTypeName(i)
FROM generateRandom('i Tuple(Int32, Array(Int64))')
LIMIT 1;
SELECT
  i
FROM generateRandom('i Tuple(Int32, Array(Int64))', 1, 10, 10)
LIMIT 10;
SELECT '-';
SELECT
  toTypeName(i)
FROM generateRandom('i Nullable(String)', 1)
LIMIT 1;
SELECT
  i
FROM generateRandom('i Nullable(String)', 1, 10, 10)
LIMIT 10;
SELECT '-';
SELECT
  toTypeName(i)
FROM generateRandom('i Array(String)')
LIMIT 1;
SELECT
  i
FROM generateRandom('i Array(String)', 1, 10, 10)
LIMIT 10;

SELECT '-';
SELECT
    toTypeName(i)
FROM generateRandom('i UUID')
LIMIT 1;
SELECT
    i
FROM generateRandom('i UUID', 1, 10, 10)
LIMIT 10;
SELECT '-';
SELECT
    toTypeName(i)
FROM generateRandom('i Array(Nullable(UUID))')
LIMIT 1;
SELECT
    i
FROM generateRandom('i Array(Nullable(UUID))', 1, 10, 10)
LIMIT 10;
SELECT '-';
SELECT
    toTypeName(i)
FROM generateRandom('i FixedString(4)')
LIMIT 1;
SELECT
    hex(i)
FROM generateRandom('i FixedString(4)', 1, 10, 10)
LIMIT 10;
SELECT '-';
SELECT
    toTypeName(i)
FROM generateRandom('i String')
LIMIT 1;
SELECT
    i
FROM generateRandom('i String', 1, 10, 10)
LIMIT 10;
SELECT '-';
DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table(a Array(Int8), d Decimal32(4), c Tuple(DateTime64(3), UUID)) ENGINE=Memory;
INSERT INTO test_table SELECT * FROM generateRandom('a Array(Int8), d Decimal32(4), c Tuple(DateTime64(3), UUID)', 1, 10, 2)
LIMIT 10;

SELECT * FROM test_table ORDER BY a, d, c;

DROP TABLE IF EXISTS test_table;

SELECT '-';

DROP TABLE IF EXISTS test_table_2;
CREATE TABLE test_table_2(a Array(Int8), b UInt32, c Nullable(String), d Decimal32(4), e Nullable(Enum16('h' = 1, 'w' = 5 , 'o' = -200)), f Float64, g Tuple(Date, DateTime, DateTime64, UUID), h FixedString(2)) ENGINE=Memory;
INSERT INTO test_table_2 SELECT * FROM generateRandom('a Array(Int8), b UInt32, c Nullable(String), d Decimal32(4), e Nullable(Enum16(\'h\' = 1, \'w\' = 5 , \'o\' = -200)), f Float64, g Tuple(Date, DateTime, DateTime64, UUID), h FixedString(2)', 10, 5, 3)
LIMIT 10;

SELECT a, b, c, d, e, f, g, hex(h) FROM test_table_2 ORDER BY a, b, c, d, e, f, g, h;
SELECT '-';

DROP TABLE IF EXISTS test_table_2;

