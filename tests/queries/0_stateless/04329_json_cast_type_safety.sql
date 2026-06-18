-- Tags: no-fasttest
-- Test that optimized JSON-to-JSON conversion (via accurateCast) produces the
-- same results as the format+parse path for all type combinations allowed by
-- isDynamicElementToTypedCastSafe.
--
-- For each case we run the conversion twice — once with the optimized path and
-- once with format+parse — and compare the output.

SET allow_experimental_json_type = 1;

-- ============================================================================
-- 1. Numeric Dynamic → Numeric typed path (accurateCast, values that fit)
-- ============================================================================

SELECT '--- Int64 dynamic → Int32 typed (values fit) ---';
DROP TABLE IF EXISTS t;
CREATE TABLE t (data JSON) ENGINE = Memory;
INSERT INTO t VALUES ('{"a": 1, "b": 1}'), ('{"a": -100, "b": 2}'), ('{"a": 2147483647, "b": 3}'), ('{"a": -2147483648, "b": 4}');
SELECT 'optimized:', (data::JSON(a Int32)).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 1;
SELECT 'format_parse:', (data::JSON(a Int32)).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 0;
DROP TABLE t;

SELECT '--- Int64 dynamic → UInt8 typed (values fit) ---';
DROP TABLE IF EXISTS t;
CREATE TABLE t (data JSON) ENGINE = Memory;
INSERT INTO t VALUES ('{"a": 0, "b": 1}'), ('{"a": 1, "b": 2}'), ('{"a": 255, "b": 3}');
SELECT 'optimized:', (data::JSON(a UInt8)).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 1;
SELECT 'format_parse:', (data::JSON(a UInt8)).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 0;
DROP TABLE t;

SELECT '--- Int64 dynamic → Float64 typed ---';
DROP TABLE IF EXISTS t;
CREATE TABLE t (data JSON) ENGINE = Memory;
INSERT INTO t VALUES ('{"a": 42, "b": 1}'), ('{"a": -100, "b": 2}'), ('{"a": 0, "b": 3}');
SELECT 'optimized:', (data::JSON(a Float64)).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 1;
SELECT 'format_parse:', (data::JSON(a Float64)).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 0;
DROP TABLE t;

SELECT '--- Float64 dynamic → Int32 typed (exact values) ---';
DROP TABLE IF EXISTS t;
CREATE TABLE t (data JSON) ENGINE = Memory;
INSERT INTO t VALUES ('{"a": 1.0, "b": 1}'), ('{"a": -100.0, "b": 2}'), ('{"a": 0.0, "b": 3}');
SELECT 'optimized:', (data::JSON(a Int32)).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 1;
SELECT 'format_parse:', (data::JSON(a Int32)).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 0;
DROP TABLE t;

SELECT '--- Bool dynamic → UInt8 typed ---';
DROP TABLE IF EXISTS t;
CREATE TABLE t (data JSON) ENGINE = Memory;
INSERT INTO t VALUES ('{"a": true, "b": 1}'), ('{"a": false, "b": 2}');
SELECT 'optimized:', (data::JSON(a UInt8)).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 1;
SELECT 'format_parse:', (data::JSON(a UInt8)).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 0;
DROP TABLE t;

SELECT '--- Bool dynamic → Int64 typed ---';
DROP TABLE IF EXISTS t;
CREATE TABLE t (data JSON) ENGINE = Memory;
INSERT INTO t VALUES ('{"a": true, "b": 1}'), ('{"a": false, "b": 2}');
SELECT 'optimized:', (data::JSON(a Int64)).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 1;
SELECT 'format_parse:', (data::JSON(a Int64)).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 0;
DROP TABLE t;

SELECT '--- UInt64 dynamic → Int64 typed (values fit) ---';
DROP TABLE IF EXISTS t;
CREATE TABLE t (data JSON) ENGINE = Memory;
-- UInt64 is inferred for large positive integers that don't fit Int64
-- But values that fit Int64 are inferred as Int64, so let's just use values that fit.
INSERT INTO t VALUES ('{"a": 0, "b": 1}'), ('{"a": 100, "b": 2}'), ('{"a": 9223372036854775807, "b": 3}');
SELECT 'optimized:', (data::JSON(a Int64)).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 1;
SELECT 'format_parse:', (data::JSON(a Int64)).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 0;
DROP TABLE t;

-- ============================================================================
-- 2. Numeric Dynamic → String typed path
-- ============================================================================

SELECT '--- Int64 dynamic → String typed ---';
DROP TABLE IF EXISTS t;
CREATE TABLE t (data JSON) ENGINE = Memory;
INSERT INTO t VALUES ('{"a": 42, "b": 1}'), ('{"a": -100, "b": 2}'), ('{"a": 0, "b": 3}');
SELECT 'optimized:', (data::JSON(a String)).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 1;
SELECT 'format_parse:', (data::JSON(a String)).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 0;
DROP TABLE t;

SELECT '--- Float64 dynamic → String typed ---';
DROP TABLE IF EXISTS t;
CREATE TABLE t (data JSON) ENGINE = Memory;
INSERT INTO t VALUES ('{"a": 1.5, "b": 1}'), ('{"a": -3.14, "b": 2}'), ('{"a": 0.0, "b": 3}');
SELECT 'optimized:', (data::JSON(a String)).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 1;
SELECT 'format_parse:', (data::JSON(a String)).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 0;
DROP TABLE t;

SELECT '--- Bool dynamic → String typed ---';
DROP TABLE IF EXISTS t;
CREATE TABLE t (data JSON) ENGINE = Memory;
INSERT INTO t VALUES ('{"a": true, "b": 1}'), ('{"a": false, "b": 2}');
SELECT 'optimized:', (data::JSON(a String)).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 1;
SELECT 'format_parse:', (data::JSON(a String)).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 0;
DROP TABLE t;

-- ============================================================================
-- 3. String Dynamic → various typed paths
-- ============================================================================

SELECT '--- String dynamic → Int64 typed ---';
DROP TABLE IF EXISTS t;
CREATE TABLE t (data JSON) ENGINE = Memory;
INSERT INTO t VALUES ('{"a": "123", "b": 1}'), ('{"a": "-456", "b": 2}'), ('{"a": "0", "b": 3}');
SELECT 'optimized:', (data::JSON(a Int64)).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 1;
SELECT 'format_parse:', (data::JSON(a Int64)).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 0;
DROP TABLE t;

SELECT '--- String dynamic → Float64 typed ---';
DROP TABLE IF EXISTS t;
CREATE TABLE t (data JSON) ENGINE = Memory;
INSERT INTO t VALUES ('{"a": "1.5", "b": 1}'), ('{"a": "-3.14", "b": 2}'), ('{"a": "0.0", "b": 3}');
SELECT 'optimized:', (data::JSON(a Float64)).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 1;
SELECT 'format_parse:', (data::JSON(a Float64)).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 0;
DROP TABLE t;

SELECT '--- String dynamic → UInt8 typed (values fit) ---';
DROP TABLE IF EXISTS t;
CREATE TABLE t (data JSON) ENGINE = Memory;
INSERT INTO t VALUES ('{"a": "0", "b": 1}'), ('{"a": "255", "b": 2}'), ('{"a": "128", "b": 3}');
SELECT 'optimized:', (data::JSON(a UInt8)).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 1;
SELECT 'format_parse:', (data::JSON(a UInt8)).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 0;
DROP TABLE t;

SELECT '--- String dynamic → Int8 typed (values fit) ---';
DROP TABLE IF EXISTS t;
CREATE TABLE t (data JSON) ENGINE = Memory;
INSERT INTO t VALUES ('{"a": "-128", "b": 1}'), ('{"a": "0", "b": 2}'), ('{"a": "127", "b": 3}');
SELECT 'optimized:', (data::JSON(a Int8)).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 1;
SELECT 'format_parse:', (data::JSON(a Int8)).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 0;
DROP TABLE t;

SELECT '--- String dynamic → Int32 typed (values fit) ---';
DROP TABLE IF EXISTS t;
CREATE TABLE t (data JSON) ENGINE = Memory;
INSERT INTO t VALUES ('{"a": "2147483647", "b": 1}'), ('{"a": "-2147483648", "b": 2}'), ('{"a": "0", "b": 3}');
SELECT 'optimized:', (data::JSON(a Int32)).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 1;
SELECT 'format_parse:', (data::JSON(a Int32)).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 0;
DROP TABLE t;

SELECT '--- String dynamic → Decimal typed ---';
DROP TABLE IF EXISTS t;
CREATE TABLE t (data JSON) ENGINE = Memory;
INSERT INTO t VALUES ('{"a": "1.23", "b": 1}'), ('{"a": "45.678", "b": 2}'), ('{"a": "-99.9", "b": 3}');
SELECT 'optimized:', (data::JSON(a Decimal64(3))).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 1;
SELECT 'format_parse:', (data::JSON(a Decimal64(3))).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 0;
DROP TABLE t;

SELECT '--- String dynamic → Date typed ---';
DROP TABLE IF EXISTS t;
CREATE TABLE t (data JSON) ENGINE = Memory;
-- With try_infer_dates=0, date-like strings stay as String variant in Dynamic.
INSERT INTO t SETTINGS input_format_try_infer_dates = 0, input_format_try_infer_datetimes = 0 VALUES ('{"a": "2024-01-15", "b": 1}'), ('{"a": "2024-06-01", "b": 2}'), ('{"a": "2025-12-31", "b": 3}');
SELECT 'optimized:', (data::JSON(a Date)).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 1;
SELECT 'format_parse:', (data::JSON(a Date)).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 0;
DROP TABLE t;

SELECT '--- String dynamic → DateTime64 typed ---';
DROP TABLE IF EXISTS t;
CREATE TABLE t (data JSON) ENGINE = Memory;
INSERT INTO t SETTINGS input_format_try_infer_datetimes = 0 VALUES ('{"a": "2024-01-15 12:30:00", "b": 1}'), ('{"a": "2024-06-01 00:00:00", "b": 2}');
SELECT 'optimized:', (data::JSON(a DateTime64(3))).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 1;
SELECT 'format_parse:', (data::JSON(a DateTime64(3))).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 0;
DROP TABLE t;

SELECT '--- String dynamic → UUID typed ---';
DROP TABLE IF EXISTS t;
CREATE TABLE t (data JSON) ENGINE = Memory;
INSERT INTO t VALUES ('{"a": "550e8400-e29b-41d4-a716-446655440000", "b": 1}'), ('{"a": "6ba7b810-9dad-11d1-80b4-00c04fd430c8", "b": 2}');
SELECT 'optimized:', (data::JSON(a UUID)).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 1;
SELECT 'format_parse:', (data::JSON(a UUID)).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 0;
DROP TABLE t;

SELECT '--- String dynamic → IPv4 typed ---';
DROP TABLE IF EXISTS t;
CREATE TABLE t (data JSON) ENGINE = Memory;
INSERT INTO t VALUES ('{"a": "192.168.1.1", "b": 1}'), ('{"a": "10.0.0.1", "b": 2}');
SELECT 'optimized:', (data::JSON(a IPv4)).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 1;
SELECT 'format_parse:', (data::JSON(a IPv4)).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 0;
DROP TABLE t;

-- ============================================================================
-- 4. Date/DateTime Dynamic → Date/DateTime typed paths
-- ============================================================================

SELECT '--- Date dynamic → String typed ---';
DROP TABLE IF EXISTS t;
CREATE TABLE t (data JSON) ENGINE = Memory;
INSERT INTO t SETTINGS input_format_try_infer_dates = 1 VALUES ('{"a": "2024-01-15", "b": 1}'), ('{"a": "2024-06-01", "b": 2}');
SELECT 'optimized:', (data::JSON(a String)).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 1;
SELECT 'format_parse:', (data::JSON(a String)).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 0;
DROP TABLE t;

SELECT '--- DateTime64 dynamic → String typed ---';
DROP TABLE IF EXISTS t;
CREATE TABLE t (data JSON) ENGINE = Memory;
INSERT INTO t SETTINGS input_format_try_infer_datetimes = 1 VALUES ('{"a": "2024-01-15 12:30:45.123", "b": 1}'), ('{"a": "2024-06-01 00:00:00", "b": 2}');
SELECT 'optimized:', (data::JSON(a String)).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 1;
SELECT 'format_parse:', (data::JSON(a String)).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 0;
DROP TABLE t;

-- ============================================================================
-- 5. Disallowed: Numeric → Date/DateTime (should fall back, both paths match)
-- ============================================================================

SELECT '--- DISALLOWED: Int64 dynamic → Date typed (fallback) ---';
DROP TABLE IF EXISTS t;
CREATE TABLE t (data JSON) ENGINE = Memory;
INSERT INTO t VALUES ('{"a": 19000, "b": 1}'), ('{"a": 19500, "b": 2}');
SELECT 'optimized:', (data::JSON(a Date)).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 1; -- {serverError INCORRECT_DATA}
SELECT 'format_parse:', (data::JSON(a Date)).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 0; -- {serverError INCORRECT_DATA}
DROP TABLE t;

SELECT '--- DISALLOWED: Int64 dynamic → DateTime typed (fallback) ---';
DROP TABLE IF EXISTS t;
CREATE TABLE t (data JSON) ENGINE = Memory;
INSERT INTO t VALUES ('{"a": 1700000000, "b": 1}'), ('{"a": 1000000000, "b": 2}');
SELECT 'optimized:', (data::JSON(a DateTime)).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 1;
SELECT 'format_parse:', (data::JSON(a DateTime)).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 0;
DROP TABLE t;

SELECT '--- DISALLOWED: Int64 dynamic → DateTime64 typed (fallback) ---';
DROP TABLE IF EXISTS t;
CREATE TABLE t (data JSON) ENGINE = Memory;
INSERT INTO t VALUES ('{"a": 1700000000, "b": 1}'), ('{"a": 1000000000, "b": 2}');
SELECT 'optimized:', (data::JSON(a DateTime64(3))).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 1;
SELECT 'format_parse:', (data::JSON(a DateTime64(3))).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 0;
DROP TABLE t;

-- ============================================================================
-- 6. Disallowed: Date/DateTime → Numeric (should fall back)
-- ============================================================================

SELECT '--- DISALLOWED: Date dynamic → Int64 typed (fallback) ---';
DROP TABLE IF EXISTS t;
CREATE TABLE t (data JSON) ENGINE = Memory;
INSERT INTO t SETTINGS input_format_try_infer_dates = 1 VALUES ('{"a": "2024-01-15", "b": 1}'), ('{"a": "2024-06-01", "b": 2}');
SELECT 'optimized:', (data::JSON(a Int64)).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 1; -- {serverError INCORRECT_DATA}
SELECT 'format_parse:', (data::JSON(a Int64)).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 0; -- {serverError INCORRECT_DATA}
DROP TABLE t;

SELECT '--- DISALLOWED: DateTime64 dynamic → Int64 typed (fallback) ---';
DROP TABLE IF EXISTS t;
CREATE TABLE t (data JSON) ENGINE = Memory;
INSERT INTO t SETTINGS input_format_try_infer_datetimes = 1 VALUES ('{"a": "2024-01-15 12:00:00", "b": 1}'), ('{"a": "2024-06-01 00:00:00", "b": 2}');
SELECT 'optimized:', (data::JSON(a Int64)).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 1; -- {serverError INCORRECT_DATA}
SELECT 'format_parse:', (data::JSON(a Int64)).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 0; -- {serverError INCORRECT_DATA}
DROP TABLE t;

SELECT '--- DISALLOWED: DateTime64 dynamic → Float64 typed (fallback) ---';
DROP TABLE IF EXISTS t;
CREATE TABLE t (data JSON) ENGINE = Memory;
INSERT INTO t SETTINGS input_format_try_infer_datetimes = 1 VALUES ('{"a": "2024-01-15 12:00:00", "b": 1}'), ('{"a": "2024-06-01 00:00:00", "b": 2}');
SELECT 'optimized:', (data::JSON(a Float64)).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 1; -- {serverError INCORRECT_DATA}
SELECT 'format_parse:', (data::JSON(a Float64)).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 0; -- {serverError INCORRECT_DATA}
DROP TABLE t;

SELECT '--- DISALLOWED: Date dynamic → DateTime typed (fallback) ---';
DROP TABLE IF EXISTS t;
CREATE TABLE t (data JSON) ENGINE = Memory;
INSERT INTO t SETTINGS input_format_try_infer_dates = 1 VALUES ('{"a": "2024-01-15", "b": 1}'), ('{"a": "2024-06-01", "b": 2}');
SELECT 'optimized:', (data::JSON(a DateTime)).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 1;
SELECT 'format_parse:', (data::JSON(a DateTime)).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 0;
DROP TABLE t;

SELECT '--- DISALLOWED: DateTime64 dynamic → Date typed (fallback) ---';
DROP TABLE IF EXISTS t;
CREATE TABLE t (data JSON) ENGINE = Memory;
INSERT INTO t SETTINGS input_format_try_infer_datetimes = 1 VALUES ('{"a": "2024-01-15 12:30:00", "b": 1}'), ('{"a": "2024-06-01 00:00:00", "b": 2}');
SELECT 'optimized:', (data::JSON(a Date)).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 1;  -- {serverError INCORRECT_DATA}
SELECT 'format_parse:', (data::JSON(a Date)).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 0; -- {serverError INCORRECT_DATA}
DROP TABLE t;

SELECT '--- DISALLOWED: DateTime64 dynamic → DateTime typed (fallback) ---';
DROP TABLE IF EXISTS t;
CREATE TABLE t (data JSON) ENGINE = Memory;
INSERT INTO t SETTINGS input_format_try_infer_datetimes = 1 VALUES ('{"a": "2024-01-15 12:30:45", "b": 1}'), ('{"a": "2024-06-01 00:00:00", "b": 2}');
SELECT 'optimized:', (data::JSON(a DateTime)).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 1;
SELECT 'format_parse:', (data::JSON(a DateTime)).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 0;
DROP TABLE t;

-- ============================================================================
-- 7. Disallowed: Float narrowing and Numeric → Decimal (should fall back)
-- ============================================================================

SELECT '--- DISALLOWED: Float64 dynamic → Float32 typed (fallback, precision loss) ---';
DROP TABLE IF EXISTS t;
CREATE TABLE t (data JSON) ENGINE = Memory;
INSERT INTO t VALUES ('{"a": 1.5, "b": 1}'), ('{"a": -3.14, "b": 2}'), ('{"a": 0.0, "b": 3}');
SELECT 'optimized:', (data::JSON(a Float32)).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 1;
SELECT 'format_parse:', (data::JSON(a Float32)).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 0;
DROP TABLE t;

SELECT '--- DISALLOWED: Int64 dynamic → Decimal typed (fallback) ---';
DROP TABLE IF EXISTS t;
CREATE TABLE t (data JSON) ENGINE = Memory;
INSERT INTO t VALUES ('{"a": 42, "b": 1}'), ('{"a": -100, "b": 2}'), ('{"a": 0, "b": 3}');
SELECT 'optimized:', (data::JSON(a Decimal64(3))).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 1;
SELECT 'format_parse:', (data::JSON(a Decimal64(3))).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 0;
DROP TABLE t;

SELECT '--- DISALLOWED: Float64 dynamic → Decimal typed (fallback) ---';
DROP TABLE IF EXISTS t;
CREATE TABLE t (data JSON) ENGINE = Memory;
INSERT INTO t VALUES ('{"a": 1.5, "b": 1}'), ('{"a": 3.14, "b": 2}');
SELECT 'optimized:', (data::JSON(a Decimal64(3))).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 1;
SELECT 'format_parse:', (data::JSON(a Decimal64(3))).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 0;
DROP TABLE t;

-- ============================================================================
-- 8. Array of safe → Array of safe
-- ============================================================================

SELECT '--- Array(Int64) dynamic → Array(Int32) typed ---';
DROP TABLE IF EXISTS t;
CREATE TABLE t (data JSON) ENGINE = Memory;
INSERT INTO t VALUES ('{"a": [1, 2, 3], "b": 1}'), ('{"a": [100, -200], "b": 2}'), ('{"a": [], "b": 3}');
SELECT 'optimized:', (data::JSON(a Array(Int32))).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 1;
SELECT 'format_parse:', (data::JSON(a Array(Int32))).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 0;
DROP TABLE t;

SELECT '--- Array(Int64) dynamic → Array(String) typed ---';
DROP TABLE IF EXISTS t;
CREATE TABLE t (data JSON) ENGINE = Memory;
INSERT INTO t VALUES ('{"a": [1, 2], "b": 1}'), ('{"a": [42], "b": 2}');
SELECT 'optimized:', (data::JSON(a Array(String))).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 1;
SELECT 'format_parse:', (data::JSON(a Array(String))).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 0;
DROP TABLE t;

-- ============================================================================
-- 9. Mixed Dynamic variants in same column (canCastDynamicToTyped checks all)
-- ============================================================================

SELECT '--- Mixed Int64+String dynamic → String typed (all variants castable) ---';
DROP TABLE IF EXISTS t;
CREATE TABLE t (data JSON) ENGINE = Memory;
INSERT INTO t VALUES ('{"a": 42, "b": 1}'), ('{"a": "hello", "b": 2}'), ('{"a": -100, "b": 3}');
SELECT 'optimized:', (data::JSON(a String)).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 1;
SELECT 'format_parse:', (data::JSON(a String)).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 0;
DROP TABLE t;

SELECT '--- Mixed Int64+Bool dynamic → Int32 typed (all variants castable) ---';
DROP TABLE IF EXISTS t;
CREATE TABLE t (data JSON) ENGINE = Memory;
INSERT INTO t VALUES ('{"a": 42, "b": 1}'), ('{"a": true, "b": 2}'), ('{"a": false, "b": 3}');
SELECT 'optimized:', (data::JSON(a Int32)).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 1;
SELECT 'format_parse:', (data::JSON(a Int32)).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 0;
DROP TABLE t;

SELECT '--- Mixed Int64+Date dynamic → String typed (all variants castable to String) ---';
DROP TABLE IF EXISTS t;
CREATE TABLE t (data JSON) ENGINE = Memory;
INSERT INTO t SETTINGS input_format_try_infer_dates = 1 VALUES ('{"a": 42, "b": 1}'), ('{"a": "2024-01-15", "b": 2}');
SELECT 'optimized:', (data::JSON(a String)).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 1;
SELECT 'format_parse:', (data::JSON(a String)).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 0;
DROP TABLE t;

SELECT '--- Mixed Int64+Date dynamic → Int32 typed (Date not castable to Int → fallback) ---';
DROP TABLE IF EXISTS t;
CREATE TABLE t (data JSON) ENGINE = Memory;
INSERT INTO t SETTINGS input_format_try_infer_dates = 1 VALUES ('{"a": 42, "b": 1}'), ('{"a": "2024-01-15", "b": 2}');
SELECT 'optimized:', (data::JSON(a Int32)).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 1; -- {serverError INCORRECT_DATA}
SELECT 'format_parse:', (data::JSON(a Int32)).a FROM t ORDER BY data.b::Int64 SETTINGS json_use_optimized_type_conversion = 0; -- {serverError INCORRECT_DATA}
DROP TABLE t;

-- ============================================================================
-- 10. Overflow behavior: accurateCast is stricter than format+parse
-- The optimized path throws on overflow where format+parse may silently wrap.
-- ============================================================================

SELECT '--- Overflow: Int64(300) → UInt8 typed ---';
DROP TABLE IF EXISTS t;
CREATE TABLE t (data JSON) ENGINE = Memory;
INSERT INTO t VALUES ('{"a": 300, "b": 1}');
-- The optimized path uses accurateCast which throws on overflow.
SELECT (data::JSON(a UInt8)).a FROM t SETTINGS json_use_optimized_type_conversion = 1; -- { serverError CANNOT_CONVERT_TYPE }
DROP TABLE t;

SELECT '--- Overflow: Int64(-1) → UInt32 typed ---';
DROP TABLE IF EXISTS t;
CREATE TABLE t (data JSON) ENGINE = Memory;
INSERT INTO t VALUES ('{"a": -1, "b": 1}');
SELECT (data::JSON(a UInt32)).a FROM t SETTINGS json_use_optimized_type_conversion = 1; -- { serverError CANNOT_CONVERT_TYPE }
DROP TABLE t;

SELECT '--- Overflow: Int64(2147483648) → Int32 typed ---';
DROP TABLE IF EXISTS t;
CREATE TABLE t (data JSON) ENGINE = Memory;
INSERT INTO t VALUES ('{"a": 2147483648, "b": 1}');
SELECT (data::JSON(a Int32)).a FROM t SETTINGS json_use_optimized_type_conversion = 1; -- { serverError CANNOT_CONVERT_TYPE }
DROP TABLE t;

SELECT '--- Overflow: Float64(1.5) → Int32 typed (non-exact) ---';
DROP TABLE IF EXISTS t;
CREATE TABLE t (data JSON) ENGINE = Memory;
INSERT INTO t VALUES ('{"a": 1.5, "b": 1}');
SELECT (data::JSON(a Int32)).a FROM t SETTINGS json_use_optimized_type_conversion = 1; -- { serverError CANNOT_CONVERT_TYPE }
DROP TABLE t;

SELECT '--- Overflow: String("300") → UInt8 typed ---';
DROP TABLE IF EXISTS t;
CREATE TABLE t (data JSON) ENGINE = Memory;
INSERT INTO t VALUES ('{"a": "300", "b": 1}');
SELECT (data::JSON(a UInt8)).a FROM t SETTINGS json_use_optimized_type_conversion = 1; -- { serverError CANNOT_PARSE_TEXT }
DROP TABLE t;

SELECT '--- Overflow: String("-1") → UInt32 typed ---';
DROP TABLE IF EXISTS t;
CREATE TABLE t (data JSON) ENGINE = Memory;
INSERT INTO t VALUES ('{"a": "-1", "b": 1}');
SELECT (data::JSON(a UInt32)).a FROM t SETTINGS json_use_optimized_type_conversion = 1; -- { serverError CANNOT_PARSE_TEXT }
DROP TABLE t;

SELECT '--- Overflow: String("99999999999") → Int32 typed ---';
DROP TABLE IF EXISTS t;
CREATE TABLE t (data JSON) ENGINE = Memory;
INSERT INTO t VALUES ('{"a": "99999999999", "b": 1}');
SELECT (data::JSON(a Int32)).a FROM t SETTINGS json_use_optimized_type_conversion = 1; -- { serverError CANNOT_PARSE_TEXT }
DROP TABLE t;

SELECT '--- Overflow: String("-200") → Int8 typed ---';
DROP TABLE IF EXISTS t;
CREATE TABLE t (data JSON) ENGINE = Memory;
INSERT INTO t VALUES ('{"a": "-200", "b": 1}');
SELECT (data::JSON(a Int8)).a FROM t SETTINGS json_use_optimized_type_conversion = 1; -- { serverError CANNOT_PARSE_TEXT }
DROP TABLE t;
