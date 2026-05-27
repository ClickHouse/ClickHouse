-- Tags: no-fasttest
-- Test that JSONExtractRaw and JSONHas work correctly for typed JSON paths
-- when the value equals the type's default (0 for UInt32, '' for String).
-- https://github.com/ClickHouse/ClickHouse/issues/101721

SET enable_json_type = 1;

DROP TABLE IF EXISTS test_json_typed_defaults;
CREATE TABLE test_json_typed_defaults (json JSON(a UInt32, b String)) ENGINE = Memory;
INSERT INTO test_json_typed_defaults VALUES ('{"a": 0, "b": ""}'), ('{"a": 42, "b": "hello"}'), ('{"a": 0, "b": ""}');

SELECT 'JSONExtractRaw';
SELECT JSONExtractRaw(json, 'a') AS raw_a, JSONExtractRaw(json, 'b') AS raw_b FROM test_json_typed_defaults ORDER BY rowNumberInAllBlocks();

SELECT 'JSONHas';
SELECT JSONHas(json, 'a') AS has_a, JSONHas(json, 'b') AS has_b FROM test_json_typed_defaults ORDER BY rowNumberInAllBlocks();

SELECT 'JSONExtractInt';
SELECT JSONExtractInt(json, 'a') AS int_a FROM test_json_typed_defaults ORDER BY rowNumberInAllBlocks();

-- Also test with absent typed paths (key missing from input JSON).
TRUNCATE TABLE test_json_typed_defaults;
INSERT INTO test_json_typed_defaults VALUES ('{"c": 1}');

SELECT 'absent typed path';
SELECT JSONExtractRaw(json, 'a') AS raw_a, JSONExtractRaw(json, 'b') AS raw_b FROM test_json_typed_defaults;
SELECT JSONHas(json, 'a') AS has_a, JSONHas(json, 'b') AS has_b FROM test_json_typed_defaults;

DROP TABLE test_json_typed_defaults;

-- Test with Nullable typed paths: NULL is a valid present value, not absence.
DROP TABLE IF EXISTS test_json_nullable_typed;
CREATE TABLE test_json_nullable_typed (json JSON(a Nullable(UInt32), b Nullable(String))) ENGINE = Memory;
INSERT INTO test_json_nullable_typed VALUES ('{"a": null, "b": null}'), ('{"a": 0, "b": ""}'), ('{"a": 42, "b": "hello"}');

SELECT 'Nullable typed path JSONExtractRaw';
SELECT JSONExtractRaw(json, 'a') AS raw_a, JSONExtractRaw(json, 'b') AS raw_b FROM test_json_nullable_typed ORDER BY rowNumberInAllBlocks();

SELECT 'Nullable typed path JSONHas';
SELECT JSONHas(json, 'a') AS has_a, JSONHas(json, 'b') AS has_b FROM test_json_nullable_typed ORDER BY rowNumberInAllBlocks();

DROP TABLE test_json_nullable_typed;
