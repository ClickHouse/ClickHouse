DROP TABLE IF EXISTS midpoint_test;
CREATE TABLE midpoint_test
(
    ui8  UInt8,
    ui16 UInt16,
    ui32 UInt32,
    i8   Int8,
    i16  Int16,
    i32  Int32,
    f32  Float32,
    f64  Float64,
    d32  Decimal32(3),
    d64  Decimal64(3),
)
ENGINE = Memory;

INSERT INTO midpoint_test VALUES
    (1, 10, 100, -1, -10, -100, 1.5, 10.5, 1.234, 10.987),
    (100, 200, 300, 50, 150, 250, 10.0, 20.0, 100.123, 200.456);
    
-- ===============================================================
-- Integer types (signed, unsigned, mixed)
-- ===============================================================

SELECT midpoint(ui8, ui16) AS result, toTypeName(result) AS type FROM midpoint_test;
SELECT midpoint(i8, i16) AS result, toTypeName(result) AS type FROM midpoint_test;
SELECT midpoint(ui32, i32) AS result, toTypeName(result) AS type FROM midpoint_test;

-- With 3 args
SELECT midpoint(i8, i16, i32) AS result, toTypeName(result) AS type FROM midpoint_test;

-- ===============================================================
-- Floating-point types
-- ===============================================================

SELECT midpoint(f32, f64) AS result, toTypeName(result) AS type FROM midpoint_test;

-- 3 args (float)
SELECT midpoint(f32, f64, 42.0) AS result, toTypeName(result) AS type FROM midpoint_test;

-- ===============================================================
-- Mixed integer + float
-- ===============================================================

SELECT midpoint(i32, f32) AS result, toTypeName(result) AS type FROM midpoint_test;
SELECT midpoint(f64, ui16, 122) AS result, toTypeName(result) AS type FROM midpoint_test;

-- ===============================================================
-- Decimal types
-- ===============================================================

SELECT midpoint(d32, d64) AS result, toTypeName(result) AS type FROM midpoint_test;

-- Mixed decimal + integer
SELECT midpoint(d32, i32) AS result, toTypeName(result) AS type FROM midpoint_test;

-- 3 args mixed decimal + int
SELECT midpoint(d64, 20, i32) AS result, toTypeName(result) AS type FROM midpoint_test;

-- ===============================================================
-- Temporal types types
-- ===============================================================

SELECT midpoint(toDate('2025-01-01'), toDate('2025-01-05')) AS result, toTypeName(result) AS type;
SELECT midpoint(toDateTime('2025-01-01 00:00:00'), toDateTime('2025-01-03 12:00:00')) AS result, toTypeName(result) AS type;
SELECT midpoint(toTime64('12:00:00', 0), toTime64('14:00:00', 0)) AS result, toTypeName(result) AS type;

-- ===============================================================
-- Nulls
-- ===============================================================

SELECT midpoint(123, null) AS result, toTypeName(result) AS type;
SELECT midpoint(3, 1.5, null) AS result, toTypeName(result) AS type;
SELECT midpoint(null, null) AS result, toTypeName(result) AS type;
SELECT midpoint(null, null, null) AS result, toTypeName(result) AS type;
