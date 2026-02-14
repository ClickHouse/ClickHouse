-- Test has() function for JSON/Object type
SET allow_experimental_object_type = 1;
SET allow_experimental_json_type = 1;

-- ============================================
-- Test 1: Basic functionality with Memory engine
-- ============================================
DROP TABLE IF EXISTS t_json_has;
CREATE TABLE t_json_has (id UInt64, j JSON) ENGINE = Memory;

INSERT INTO t_json_has VALUES (1, '{"hello": 123, "world": {"param": 456}}');
INSERT INTO t_json_has VALUES (2, '{"hello": 456}');
INSERT INTO t_json_has VALUES (3, '{}');

SELECT '-- Basic path existence';
SELECT id, has(j, 'hello') FROM t_json_has ORDER BY id;

SELECT '-- Nested path with dot notation';
SELECT id, has(j, 'world.param') FROM t_json_has ORDER BY id;
SELECT id, has(j, 'world') FROM t_json_has ORDER BY id;

SELECT '-- Non-existent paths';
SELECT id, has(j, 'missing') FROM t_json_has ORDER BY id;
SELECT id, has(j, 'world.missing') FROM t_json_has ORDER BY id;
SELECT id, has(j, 'hello.nested') FROM t_json_has ORDER BY id;

DROP TABLE t_json_has;

-- ============================================
-- Test 2: NULL value handling
-- ============================================
DROP TABLE IF EXISTS t_json_null;
CREATE TABLE t_json_null (id UInt64, j JSON) ENGINE = Memory;

INSERT INTO t_json_null VALUES (1, '{"a": null}');
INSERT INTO t_json_null VALUES (2, '{"a": 1}');
INSERT INTO t_json_null VALUES (3, '{"b": 2}');

SELECT '-- NULL value handling (path exists but value is null)';
SELECT id, has(j, 'a') FROM t_json_null ORDER BY id;

DROP TABLE t_json_null;

-- ============================================
-- Test 3: Constant JSON expressions
-- ============================================
SELECT '-- Constant JSON expressions';
SELECT has('{"hello": 123}'::JSON, 'hello');
SELECT has('{"hello": 123}'::JSON, 'world');
SELECT has('{"a": {"b": {"c": 1}}}'::JSON, 'a.b.c');
SELECT has('{"a": {"b": {"c": 1}}}'::JSON, 'a.b');
SELECT has('{"a": {"b": {"c": 1}}}'::JSON, 'a.b.d');
SELECT has('{}'::JSON, 'anything');

-- ============================================
-- Test 4: Edge cases
-- ============================================
DROP TABLE IF EXISTS t_json_edge;
CREATE TABLE t_json_edge (id UInt64, j JSON) ENGINE = Memory;

INSERT INTO t_json_edge VALUES (1, '{"": 1}');
INSERT INTO t_json_edge VALUES (2, '{"a": [1, 2, 3]}');
INSERT INTO t_json_edge VALUES (3, '{"a": {}}');
INSERT INTO t_json_edge VALUES (4, '{"a": []}');

SELECT '-- Empty string key';
SELECT id, has(j, '') FROM t_json_edge WHERE id = 1;

SELECT '-- Array values';
SELECT id, has(j, 'a') FROM t_json_edge WHERE id IN (2, 4) ORDER BY id;

SELECT '-- Empty nested object';
SELECT id, has(j, 'a') FROM t_json_edge WHERE id = 3;

DROP TABLE t_json_edge;

-- ============================================
-- Test 5: MergeTree engine (for subcolumn optimization)
-- ============================================
DROP TABLE IF EXISTS t_json_mt;
CREATE TABLE t_json_mt (id UInt64, j JSON) ENGINE = MergeTree ORDER BY id;

INSERT INTO t_json_mt VALUES (1, '{"x": 1, "y": {"z": 2}}');
INSERT INTO t_json_mt VALUES (2, '{"x": 2}');
INSERT INTO t_json_mt VALUES (3, '{"w": 3}');

SELECT '-- MergeTree engine test';
SELECT id, has(j, 'x') FROM t_json_mt ORDER BY id SETTINGS allow_experimental_parallel_reading_from_replicas=0;
SELECT id, has(j, 'y.z') FROM t_json_mt ORDER BY id SETTINGS allow_experimental_parallel_reading_from_replicas=0;
SELECT id, has(j, 'w') FROM t_json_mt ORDER BY id SETTINGS allow_experimental_parallel_reading_from_replicas=0;

DROP TABLE t_json_mt;

-- ============================================
-- Test 6: Variable path (non-constant second argument)
-- ============================================
DROP TABLE IF EXISTS t_json_var_path;
CREATE TABLE t_json_var_path (id UInt64, j JSON, path String) ENGINE = Memory;

INSERT INTO t_json_var_path VALUES (1, '{"a": 1, "b": 2}', 'a');
INSERT INTO t_json_var_path VALUES (2, '{"a": 1, "b": 2}', 'b');
INSERT INTO t_json_var_path VALUES (3, '{"a": 1, "b": 2}', 'c');

SELECT '-- Variable path column';
SELECT id, path, has(j, path) FROM t_json_var_path ORDER BY id;

DROP TABLE t_json_var_path;

-- ============================================
-- Test 7: Special characters in paths
-- ============================================
SELECT '-- Special characters in keys';
SELECT has('{"key-with-dash": 1}'::JSON, 'key-with-dash');
SELECT has('{"key_with_underscore": 1}'::JSON, 'key_with_underscore');
SELECT has('{"key.with.dots": 1}'::JSON, 'key.with.dots');  -- This may be tricky!
SELECT has('{"123numeric": 1}'::JSON, '123numeric');
