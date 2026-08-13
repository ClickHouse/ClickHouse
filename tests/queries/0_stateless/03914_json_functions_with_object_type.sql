-- Tests for JSONExtract* functions with native JSON type input.
SET allow_experimental_json_type = 1;

-- ==========================================================================
-- Part 1: Runtime execution tests (JSONExtract* with JSON type)
-- ==========================================================================

CREATE TABLE json_test (
    id Int32,
    json_string String,
    json_object JSON
) ENGINE = Memory;

INSERT INTO json_test VALUES
    (1, '{"name": "Alice", "age": 30, "scores": [85, 90, 95]}', '{"name": "Alice", "age": 30, "scores": [85, 90, 95]}'),
    (2, '{"name": "Bob", "age": 25, "scores": [75, 80, 88]}', '{"name": "Bob", "age": 25, "scores": [75, 80, 88]}'),
    (3, '{"name": "Charlie", "age": 35, "scores": [92, 88, 90]}', '{"name": "Charlie", "age": 35, "scores": [92, 88, 90]}');

SELECT 'Test 1: JSONExtractString';
SELECT
    id,
    JSONExtractString(json_string, 'name') as from_string,
    JSONExtractString(json_object, 'name') as from_object
FROM json_test
ORDER BY id;

SELECT 'Test 2: JSONExtractInt';
SELECT
    id,
    JSONExtractInt(json_string, 'age') as age_from_string,
    JSONExtractInt(json_object, 'age') as age_from_object
FROM json_test
ORDER BY id;

SELECT 'Test 3: JSONExtractUInt';
SELECT
    id,
    JSONExtractUInt(json_string, 'age') as age_from_string,
    JSONExtractUInt(json_object, 'age') as age_from_object
FROM json_test
ORDER BY id;

SELECT 'Test 4: JSONExtractRaw';
SELECT
    id,
    JSONExtractRaw(json_string, 'scores') as scores_from_string,
    JSONExtractRaw(json_object, 'scores') as scores_from_object
FROM json_test
ORDER BY id;

SELECT 'Test 5: JSONExtract (generic with type)';
SELECT
    id,
    JSONExtract(json_string, 'age', 'Int32') as age_from_string,
    JSONExtract(json_object, 'age', 'Int32') as age_from_object
FROM json_test
ORDER BY id;

SELECT 'Test 6: JSONExtractFloat';
DROP TABLE IF EXISTS json_float;
CREATE TABLE json_float (data JSON) ENGINE = Memory;
INSERT INTO json_float VALUES ('{"pi": 3.14159}');
SELECT JSONExtractFloat(data, 'pi') as pi FROM json_float;
DROP TABLE json_float;

SELECT 'Test 7: JSONExtractBool';
DROP TABLE IF EXISTS json_bool;
CREATE TABLE json_bool (data JSON) ENGINE = Memory;
INSERT INTO json_bool VALUES ('{"flag": true}');
SELECT JSONExtractBool(data, 'flag') as flag FROM json_bool;
DROP TABLE json_bool;

-- Cleanup
DROP TABLE json_test;

-- ==========================================================================
-- Part 2: Nested path tests
-- ==========================================================================

SELECT 'Test 8: Nested path extraction';
DROP TABLE IF EXISTS json_nested;
CREATE TABLE json_nested (data JSON) ENGINE = Memory;
INSERT INTO json_nested VALUES ('{"x": {"y": {"z": 42}}}');
INSERT INTO json_nested VALUES ('{"x": {"y": {"z": 99}}}');
SELECT JSONExtractInt(data, 'x', 'y', 'z') as val FROM json_nested ORDER BY val;
DROP TABLE json_nested;

SELECT 'Test 9: Missing path returns default';
DROP TABLE IF EXISTS json_missing;
CREATE TABLE json_missing (data JSON) ENGINE = Memory;
INSERT INTO json_missing VALUES ('{"a": 1}');
SELECT
    JSONExtractInt(data, 'nonexistent') as missing_int,
    JSONExtractString(data, 'nonexistent') as missing_str
FROM json_missing;
DROP TABLE json_missing;

SELECT 'Test 10: ColumnConst JSON object';
SELECT JSONExtractInt(CAST('{"val": 777}' AS JSON), 'val') as const_val;

-- ==========================================================================
-- Part 3: JSONExtractRaw with nested objects
-- ==========================================================================

SELECT 'Test 11: JSONExtractRaw with nested object';
DROP TABLE IF EXISTS json_raw;
CREATE TABLE json_raw (data JSON) ENGINE = Memory;
INSERT INTO json_raw VALUES ('{"a": "Hello"}');
INSERT INTO json_raw VALUES ('{"a": {"b": "World"}}');
SELECT JSONExtractRaw(data, 'a') as raw_a FROM json_raw ORDER BY raw_a;
DROP TABLE json_raw;

-- ==========================================================================
-- Part 4: Edge cases - Date types, indexes, non-const keys
-- ==========================================================================

-- Date stored as UInt16 internally; extracting as String must return "2020-01-01", not "18262".
SELECT 'Test 12: Date type extraction as String';
DROP TABLE IF EXISTS json_date;
CREATE TABLE json_date (data JSON(date Date)) ENGINE = Memory;
INSERT INTO json_date VALUES ('{"date": "2020-01-01"}');
SELECT JSONExtract(data, 'date', 'String') FROM json_date;
DROP TABLE json_date;

-- Type hint with subobject: castColumn on subobject to typed literal type must not fail.
SELECT 'Test 15: Type hint path with conflicting subobject';
SELECT JSONExtract('{"a" : 42, "a.b" : 42}'::JSON(a UInt32), 'a', 'String');

SELECT 'All tests completed';
