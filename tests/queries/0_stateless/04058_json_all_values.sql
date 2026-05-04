-- Tags: no-fasttest

SET enable_json_type = 1;

SELECT 'Basic usage';
SELECT materialize('{"a": 42}')::JSON as json, JSONAllValues(json);
SELECT materialize('{"b": "Hello"}')::JSON as json, JSONAllValues(json);
SELECT materialize('{"a": [1, 2, 3], "c": "2020-01-01"}')::JSON as json, JSONAllValues(json);

SELECT 'With max_dynamic_paths=1 (some paths go to shared data)';
SELECT materialize('{"a": 42}')::JSON(max_dynamic_paths=1) as json, JSONAllValues(json);
SELECT materialize('{"b": "Hello"}')::JSON(max_dynamic_paths=1) as json, JSONAllValues(json);
SELECT materialize('{"a": [1, 2, 3], "c": "2020-01-01"}')::JSON(max_dynamic_paths=1) as json, JSONAllValues(json);

SELECT 'With max_dynamic_paths=0 (all paths in shared data)';
SELECT materialize('{"a": 42}')::JSON(max_dynamic_paths=0) as json, JSONAllValues(json);
SELECT materialize('{"b": "Hello"}')::JSON(max_dynamic_paths=0) as json, JSONAllValues(json);
SELECT materialize('{"a": [1, 2, 3], "c": "2020-01-01"}')::JSON(max_dynamic_paths=0) as json, JSONAllValues(json);

SELECT 'With typed paths';
SELECT materialize('{"a": 42, "b": "Hello"}')::JSON(a UInt32) as json, JSONAllValues(json);
SELECT materialize('{"b": "World"}')::JSON(a UInt32) as json, JSONAllValues(json);

SELECT 'Empty JSON';
SELECT materialize('{}')::JSON as json, JSONAllValues(json);

SELECT 'Nested paths';
SELECT materialize('{"a": {"b": 1, "c": 2}, "d": 3}')::JSON as json, JSONAllValues(json);

SELECT 'Multiple types in same path across rows';
DROP TABLE IF EXISTS test_json_all_values;
CREATE TABLE test_json_all_values (json JSON(max_dynamic_paths=1)) ENGINE = Memory;
INSERT INTO test_json_all_values SELECT materialize('{"a": 42}')::JSON(max_dynamic_paths=1);
INSERT INTO test_json_all_values SELECT materialize('{"b": "Hello"}')::JSON(max_dynamic_paths=1);
INSERT INTO test_json_all_values SELECT materialize('{"a": [1, 2, 3], "c": "2020-01-01"}')::JSON(max_dynamic_paths=1);
SELECT json, JSONAllValues(json) FROM test_json_all_values ORDER BY toString(json);
DROP TABLE test_json_all_values;
