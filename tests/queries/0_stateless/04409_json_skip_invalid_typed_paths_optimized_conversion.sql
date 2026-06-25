-- Tests that type_json_skip_invalid_typed_paths works correctly with the optimized
-- JSON type conversion path (json_use_optimized_type_conversion = 1).
-- Verifies that invalid values produce defaults instead of exceptions for:
--   1. Changed typed paths (e.g. String -> UInt32)
--   2. New typed paths extracted from dynamic
--   3. New typed paths promoted from shared data

SET allow_experimental_json_type = 1;
SET type_json_skip_invalid_typed_paths = 1;
SET json_use_optimized_type_conversion = 1;

-- Case 1: Changed typed paths — value incompatible with the new type gets default.
-- Source has typed path `a String`, destination changes it to `a UInt32`.
-- Row with "bad" should get default 0, row with "123" should get 123.
SELECT '--- Changed typed paths ---';
DROP TABLE IF EXISTS t_changed;
CREATE TABLE t_changed (data JSON(a String)) ENGINE = Memory;
INSERT INTO t_changed VALUES ('{"a":"123"}'), ('{"a":"bad"}'), ('{"a":"456"}');
SELECT data::JSON(a UInt32) FROM t_changed;
SELECT (data::JSON(a UInt32)).a FROM t_changed;
DROP TABLE t_changed;

-- Case 2: New typed paths from dynamic — value in dynamic column incompatible with typed path.
-- Source has no typed path `a`, so `a` lives in dynamic. Destination declares `a UInt32`.
-- Row with "bad" should get default 0.
SELECT '--- New typed paths from dynamic ---';
DROP TABLE IF EXISTS t_dynamic;
CREATE TABLE t_dynamic (data JSON(max_dynamic_paths=10)) ENGINE = Memory;
INSERT INTO t_dynamic VALUES ('{"a":"123"}'), ('{"a":"bad"}'), ('{"a":"789"}');
SELECT data::JSON(a UInt32, max_dynamic_paths=10) FROM t_dynamic;
SELECT (data::JSON(a UInt32, max_dynamic_paths=10)).a FROM t_dynamic;
DROP TABLE t_dynamic;

-- Case 3: New typed paths from shared data — value in shared data incompatible with typed path.
-- Use max_dynamic_paths=0 to force `a` into shared data in the source.
SELECT '--- New typed paths from shared data ---';
DROP TABLE IF EXISTS t_shared;
CREATE TABLE t_shared (data JSON(max_dynamic_paths=0)) ENGINE = Memory;
INSERT INTO t_shared VALUES ('{"a":"100"}'), ('{"a":"bad"}'), ('{"a":"200"}');
SELECT data::JSON(a UInt32, max_dynamic_paths=0) FROM t_shared;
SELECT (data::JSON(a UInt32, max_dynamic_paths=0)).a FROM t_shared;
DROP TABLE t_shared;

-- Case 4: Verify that without the setting, invalid values still throw.
SET type_json_skip_invalid_typed_paths = 0;
SELECT '--- Without skip_invalid (should throw) ---';
DROP TABLE IF EXISTS t_throw;
CREATE TABLE t_throw (data JSON(a String)) ENGINE = Memory;
INSERT INTO t_throw VALUES ('{"a":"bad"}');
SELECT data::JSON(a UInt32) FROM t_throw; -- { serverError CANNOT_PARSE_TEXT }
DROP TABLE t_throw;

-- Case 5: Verify optimized path matches the legacy format+parse path.
SET type_json_skip_invalid_typed_paths = 1;
SELECT '--- Optimized vs legacy comparison ---';
DROP TABLE IF EXISTS t_compare;
CREATE TABLE t_compare (data JSON(a String)) ENGINE = Memory;
INSERT INTO t_compare VALUES ('{"a":"10"}'), ('{"a":"hello"}'), ('{"a":"20"}');

SELECT 'optimized';
SET json_use_optimized_type_conversion = 1;
SELECT (data::JSON(a UInt32)).a FROM t_compare;

SELECT 'legacy';
SET json_use_optimized_type_conversion = 0;
SELECT (data::JSON(a UInt32)).a FROM t_compare;

DROP TABLE t_compare;
