-- Test has() function for JSON type

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
-- Test 5: MergeTree engine
-- ============================================
DROP TABLE IF EXISTS t_json_mt;
CREATE TABLE t_json_mt (id UInt64, j JSON) ENGINE = MergeTree ORDER BY id;

INSERT INTO t_json_mt VALUES (1, '{"x": 1, "y": {"z": 2}}');
INSERT INTO t_json_mt VALUES (2, '{"x": 2}');
INSERT INTO t_json_mt VALUES (3, '{"w": 3}');

SELECT '-- MergeTree engine test';
SELECT id, has(j, 'x') FROM t_json_mt ORDER BY id;
SELECT id, has(j, 'y.z') FROM t_json_mt ORDER BY id;
SELECT id, has(j, 'w') FROM t_json_mt ORDER BY id;

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
SELECT has('{"key.with" : {"dots": 1}}'::JSON, 'key.with.dots');
SELECT has('{"123numeric": 1}'::JSON, '123numeric');

-- ============================================
-- Test 8: JSON with typed paths (constant second argument)
-- ============================================
DROP TABLE IF EXISTS t_json_typed;
CREATE TABLE t_json_typed (id UInt64, j JSON(a UInt32, b.c Nullable(String))) ENGINE = Memory;

INSERT INTO t_json_typed VALUES (1, '{"a": 1, "b": {"c": "hello"}}');
INSERT INTO t_json_typed VALUES (2, '{"a": 2}');
INSERT INTO t_json_typed VALUES (3, '{}');

SELECT '-- Typed paths with constant path argument';
-- Typed paths are always considered present (even if value is default/null)
SELECT id, has(j, 'a') FROM t_json_typed ORDER BY id;
SELECT id, has(j, 'b.c') FROM t_json_typed ORDER BY id;
SELECT id, has(j, 'b') FROM t_json_typed ORDER BY id;
SELECT id, has(j, 'missing') FROM t_json_typed ORDER BY id;

DROP TABLE t_json_typed;

-- ============================================
-- Test 9: JSON with typed paths (non-constant second argument)
-- ============================================
DROP TABLE IF EXISTS t_json_typed_var;
CREATE TABLE t_json_typed_var (id UInt64, j JSON(x UInt32, y.z Nullable(UInt32)), path String) ENGINE = Memory;

INSERT INTO t_json_typed_var VALUES (1, '{"x": 1, "y": {"z": 2}}', 'x');
INSERT INTO t_json_typed_var VALUES (2, '{"x": 2}', 'y.z');
INSERT INTO t_json_typed_var VALUES (3, '{}', 'y');
INSERT INTO t_json_typed_var VALUES (4, '{"x": 4}', 'missing');

SELECT '-- Typed paths with variable path argument';
SELECT id, path, has(j, path) FROM t_json_typed_var ORDER BY id;

DROP TABLE t_json_typed_var;

-- ============================================
-- Test 10: JSON with paths in shared data (max_dynamic_paths=0)
-- ============================================
DROP TABLE IF EXISTS t_json_shared;
CREATE TABLE t_json_shared (id UInt64, j JSON(max_dynamic_paths=0)) ENGINE = Memory;

INSERT INTO t_json_shared VALUES (1, '{"a": 1, "b": {"c": 2}, "d": 3}');
INSERT INTO t_json_shared VALUES (2, '{"a": 2, "e": 4}');
INSERT INTO t_json_shared VALUES (3, '{"f": 5}');

SELECT '-- Shared data paths with constant path argument';
SELECT id, has(j, 'a') FROM t_json_shared ORDER BY id;
SELECT id, has(j, 'b.c') FROM t_json_shared ORDER BY id;
SELECT id, has(j, 'b') FROM t_json_shared ORDER BY id;
SELECT id, has(j, 'd') FROM t_json_shared ORDER BY id;
SELECT id, has(j, 'e') FROM t_json_shared ORDER BY id;
SELECT id, has(j, 'f') FROM t_json_shared ORDER BY id;
SELECT id, has(j, 'missing') FROM t_json_shared ORDER BY id;

DROP TABLE t_json_shared;

-- ============================================
-- Test 11: JSON with paths in shared data (non-constant second argument)
-- ============================================
DROP TABLE IF EXISTS t_json_shared_var;
CREATE TABLE t_json_shared_var (id UInt64, j JSON(max_dynamic_paths=0), path String) ENGINE = Memory;

INSERT INTO t_json_shared_var VALUES (1, '{"a": 1, "b": {"c": 2}}', 'a');
INSERT INTO t_json_shared_var VALUES (2, '{"a": 2, "b": {"c": 3}}', 'b.c');
INSERT INTO t_json_shared_var VALUES (3, '{"a": 3}', 'b');
INSERT INTO t_json_shared_var VALUES (4, '{"d": 4}', 'd');
INSERT INTO t_json_shared_var VALUES (5, '{}', 'missing');

SELECT '-- Shared data paths with variable path argument';
SELECT id, path, has(j, path) FROM t_json_shared_var ORDER BY id;

DROP TABLE t_json_shared_var;

-- ============================================
-- Test 12: Mixed typed paths and shared data
-- ============================================
DROP TABLE IF EXISTS t_json_mixed;
CREATE TABLE t_json_mixed (id UInt64, j JSON(typed_field UInt32, max_dynamic_paths=1)) ENGINE = Memory;

INSERT INTO t_json_mixed VALUES (1, '{"typed_field": 1, "dynamic_field": 2, "shared_field": 3}');
INSERT INTO t_json_mixed VALUES (2, '{"typed_field": 2, "other_shared": 4}');
INSERT INTO t_json_mixed VALUES (3, '{}');

SELECT '-- Mixed: typed paths, dynamic paths, and shared data';
SELECT id, has(j, 'typed_field') FROM t_json_mixed ORDER BY id;
SELECT id, has(j, 'dynamic_field') FROM t_json_mixed ORDER BY id;
SELECT id, has(j, 'shared_field') FROM t_json_mixed ORDER BY id;
SELECT id, has(j, 'other_shared') FROM t_json_mixed ORDER BY id;
SELECT id, has(j, 'missing') FROM t_json_mixed ORDER BY id;

DROP TABLE t_json_mixed;

-- ============================================
-- Test 13: Deeply nested paths
-- ============================================
SELECT '-- Deeply nested paths';
SELECT has('{"a": {"b": {"c": {"d": {"e": 1}}}}}'::JSON, 'a.b.c.d.e');
SELECT has('{"a": {"b": {"c": {"d": {"e": 1}}}}}'::JSON, 'a.b.c.d');
SELECT has('{"a": {"b": {"c": {"d": {"e": 1}}}}}'::JSON, 'a.b.c');
SELECT has('{"a": {"b": {"c": {"d": {"e": 1}}}}}'::JSON, 'a.b');
SELECT has('{"a": {"b": {"c": {"d": {"e": 1}}}}}'::JSON, 'a');
SELECT has('{"a": {"b": {"c": {"d": {"e": 1}}}}}'::JSON, 'a.b.c.d.e.f');
