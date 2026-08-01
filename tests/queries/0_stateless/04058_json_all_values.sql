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

SELECT 'Complex structures with plain JSON';
DROP TABLE IF EXISTS test_json_all_values_complex;
CREATE TABLE test_json_all_values_complex (id UInt32, json JSON) ENGINE = Memory;
INSERT INTO test_json_all_values_complex
SELECT 1, materialize('{"meta":{"id":1},"a":{"b":{"c":1,"d":"alpha"}},"m":true}')::JSON;
INSERT INTO test_json_all_values_complex
SELECT 2, materialize('{"meta":{"id":2},"a":[{"b":1},{"b":2}],"k":"v"}')::JSON;
INSERT INTO test_json_all_values_complex
SELECT 3, materialize('{"meta":{"id":3},"arr":[[1,2],[3]],"obj":{"list":["x","y"],"empty":[]}}')::JSON;
INSERT INTO test_json_all_values_complex
SELECT 4, materialize('{"meta":{"id":4},"a":{"b":{"c":[1,2],"d":{"x":1}}},"m":false}')::JSON;
INSERT INTO test_json_all_values_complex
SELECT 5, materialize('{"meta":{"id":5},"nullish":null,"empty":"","space":" ","arr":[],"obj":{}}')::JSON;
INSERT INTO test_json_all_values_complex
SELECT 6, materialize('{"meta":{"id":6},"p1":1,"p2":2,"p3":3,"nested":{"a":"A","b":"B"},"arr":[{"z":0},{"z":1}]}')::JSON;
SELECT id, json, JSONAllValues(json) FROM test_json_all_values_complex ORDER BY id;
DROP TABLE test_json_all_values_complex;

SELECT 'Complex structures with max_dynamic_paths=1';
CREATE TABLE test_json_all_values_complex (id UInt32, json JSON(max_dynamic_paths=1)) ENGINE = Memory;
INSERT INTO test_json_all_values_complex
SELECT 1, materialize('{"meta":{"id":1},"a":{"b":{"c":1,"d":"alpha"}},"m":true}')::JSON(max_dynamic_paths=1);
INSERT INTO test_json_all_values_complex
SELECT 2, materialize('{"meta":{"id":2},"a":[{"b":1},{"b":2}],"k":"v"}')::JSON(max_dynamic_paths=1);
INSERT INTO test_json_all_values_complex
SELECT 3, materialize('{"meta":{"id":3},"arr":[[1,2],[3]],"obj":{"list":["x","y"],"empty":[]}}')::JSON(max_dynamic_paths=1);
INSERT INTO test_json_all_values_complex
SELECT 4, materialize('{"meta":{"id":4},"a":{"b":{"c":[1,2],"d":{"x":1}}},"m":false}')::JSON(max_dynamic_paths=1);
INSERT INTO test_json_all_values_complex
SELECT 5, materialize('{"meta":{"id":5},"nullish":null,"empty":"","space":" ","arr":[],"obj":{}}')::JSON(max_dynamic_paths=1);
INSERT INTO test_json_all_values_complex
SELECT 6, materialize('{"meta":{"id":6},"p1":1,"p2":2,"p3":3,"nested":{"a":"A","b":"B"},"arr":[{"z":0},{"z":1}]}')::JSON(max_dynamic_paths=1);
SELECT id, JSONAllValues(json), JSONDynamicPaths(json), JSONSharedDataPaths(json)
FROM test_json_all_values_complex
ORDER BY id;
DROP TABLE test_json_all_values_complex;

SELECT 'Complex structures with max_dynamic_paths=0';
CREATE TABLE test_json_all_values_complex (id UInt32, json JSON(max_dynamic_paths=0)) ENGINE = Memory;
INSERT INTO test_json_all_values_complex
SELECT 1, materialize('{"meta":{"id":1},"a":{"b":{"c":1,"d":"alpha"}},"m":true}')::JSON(max_dynamic_paths=0);
INSERT INTO test_json_all_values_complex
SELECT 2, materialize('{"meta":{"id":2},"a":[{"b":1},{"b":2}],"k":"v"}')::JSON(max_dynamic_paths=0);
INSERT INTO test_json_all_values_complex
SELECT 3, materialize('{"meta":{"id":3},"arr":[[1,2],[3]],"obj":{"list":["x","y"],"empty":[]}}')::JSON(max_dynamic_paths=0);
INSERT INTO test_json_all_values_complex
SELECT 4, materialize('{"meta":{"id":4},"a":{"b":{"c":[1,2],"d":{"x":1}}},"m":false}')::JSON(max_dynamic_paths=0);
INSERT INTO test_json_all_values_complex
SELECT 5, materialize('{"meta":{"id":5},"nullish":null,"empty":"","space":" ","arr":[],"obj":{}}')::JSON(max_dynamic_paths=0);
INSERT INTO test_json_all_values_complex
SELECT 6, materialize('{"meta":{"id":6},"p1":1,"p2":2,"p3":3,"nested":{"a":"A","b":"B"},"arr":[{"z":0},{"z":1}]}')::JSON(max_dynamic_paths=0);
SELECT id, JSONAllValues(json), JSONDynamicPaths(json), JSONSharedDataPaths(json)
FROM test_json_all_values_complex
ORDER BY id;
DROP TABLE test_json_all_values_complex;

SELECT 'Complex structures with typed path variant';
CREATE TABLE test_json_all_values_complex
(
    id UInt32,
    json JSON(max_dynamic_paths=1, meta.id UInt32)
)
ENGINE = Memory;
INSERT INTO test_json_all_values_complex
SELECT 1, materialize('{"meta":{"id":1},"a":{"b":{"c":1,"d":"alpha"}},"m":true}')::JSON(max_dynamic_paths=1, meta.id UInt32);
INSERT INTO test_json_all_values_complex
SELECT 2, materialize('{"meta":{"id":2},"a":[{"b":1},{"b":2}],"k":"v"}')::JSON(max_dynamic_paths=1, meta.id UInt32);
INSERT INTO test_json_all_values_complex
SELECT 3, materialize('{"meta":{"id":3},"arr":[[1,2],[3]],"obj":{"list":["x","y"],"empty":[]}}')::JSON(max_dynamic_paths=1, meta.id UInt32);
INSERT INTO test_json_all_values_complex
SELECT 4, materialize('{"meta":{"id":4},"a":{"b":{"c":[1,2],"d":{"x":1}}},"m":false}')::JSON(max_dynamic_paths=1, meta.id UInt32);
INSERT INTO test_json_all_values_complex
SELECT 5, materialize('{"meta":{"id":5},"nullish":null,"empty":"","space":" ","arr":[],"obj":{}}')::JSON(max_dynamic_paths=1, meta.id UInt32);
INSERT INTO test_json_all_values_complex
SELECT 6, materialize('{"meta":{"id":6},"p1":1,"p2":2,"p3":3,"nested":{"a":"A","b":"B"},"arr":[{"z":0},{"z":1}]}')::JSON(max_dynamic_paths=1, meta.id UInt32);
SELECT id, json.meta.id, JSONAllValues(json)
FROM test_json_all_values_complex
ORDER BY id;
DROP TABLE test_json_all_values_complex;
