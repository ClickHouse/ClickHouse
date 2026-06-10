SET enable_analyzer = 1;

-- ============================================
-- Setup
-- ============================================
DROP TABLE IF EXISTS test_json;
CREATE TABLE test_json (json JSON) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO test_json VALUES
('{"a": 1, "b": "hello", "c": {"d": 10, "e": [1,2,3]}}'),
('{"a": 2, "b": "world", "c": {"d": 20, "e": [4,5,6]}, "f": "extra"}'),
('{"a": 3}');

-- ============================================
-- 1. Single key access
-- ============================================
SELECT 'single_key';
SELECT json['a'] FROM test_json;
SELECT json['b'] FROM test_json;

-- ============================================
-- 2. Nested key access (chains)
-- ============================================
SELECT 'nested_key';
SELECT json['c']['d'] FROM test_json;
SELECT json['c']['e'] FROM test_json;

-- ============================================
-- 3. Missing / nonexistent keys
-- ============================================
SELECT 'missing_key';
SELECT json['nonexistent'] FROM test_json;
SELECT json['c']['nonexistent'] FROM test_json;

-- ============================================
-- 4. Typed paths
-- ============================================
DROP TABLE IF EXISTS test_json_typed;
CREATE TABLE test_json_typed (json JSON(a UInt32, b String))
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO test_json_typed VALUES
('{"a": 1, "b": "hello", "c": {"d": 10}}'),
('{"a": 2, "b": "world"}');

SELECT 'typed_paths';
SELECT json['a'], toTypeName(json['a']) FROM test_json_typed;
SELECT json['b'], toTypeName(json['b']) FROM test_json_typed;

SELECT 'dynamic_path';
SELECT json['c'], toTypeName(json['c']) FROM test_json_typed;
SELECT json['c']['d'], toTypeName(json['c']['d']) FROM test_json_typed;

-- ============================================
-- 5. Typed path return types: single key
-- ============================================
-- Verify that bracket access on a typed path returns the typed type, not Dynamic.
SELECT 'typed_path_type_optimized';
SELECT json['a'], toTypeName(json['a']) FROM test_json_typed
SETTINGS optimize_functions_to_subcolumns = 1;

SELECT 'typed_path_type_no_optimization';
SELECT json['a'], toTypeName(json['a']) FROM test_json_typed
SETTINGS optimize_functions_to_subcolumns = 0;

-- ============================================
-- 6. Typed dotted path via chained access
-- ============================================
-- json JSON(c.d UInt32): json['c']['d'] goes through Dynamic dispatch,
-- so the result type is Dynamic even though c.d is a typed path.
DROP TABLE IF EXISTS test_json_dotted;
CREATE TABLE test_json_dotted (json JSON(c.d UInt32)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO test_json_dotted VALUES
('{"c": {"d": 10, "e": "hello"}}'),
('{"c": {"d": 20}}');

SELECT 'dotted_typed_path_chained';
SELECT json['c']['d'], toTypeName(json['c']['d']) FROM test_json_dotted;

SELECT 'dotted_typed_path_dot_notation';
SELECT json.c.d, toTypeName(json.c.d) FROM test_json_dotted;

-- Non-typed dotted path via chained access
SELECT 'dotted_dynamic_path_chained';
SELECT json['c']['e'], toTypeName(json['c']['e']) FROM test_json_dotted;

-- ============================================
-- 7. EXPLAIN: optimization applied
-- ============================================
SELECT 'explain_optimized_single';
EXPLAIN QUERY TREE run_passes = 1
SELECT json['a'] FROM test_json
SETTINGS optimize_functions_to_subcolumns = 1;

SELECT 'explain_optimized_chain';
EXPLAIN QUERY TREE run_passes = 1
SELECT json['c']['d'] FROM test_json
SETTINGS optimize_functions_to_subcolumns = 1;

SELECT 'explain_no_optimization';
EXPLAIN QUERY TREE run_passes = 1
SELECT json['a'] FROM test_json
SETTINGS optimize_functions_to_subcolumns = 0;

-- EXPLAIN: typed path should show the typed type, not Dynamic
SELECT 'explain_typed_path_optimized';
EXPLAIN QUERY TREE run_passes = 1
SELECT json['a'] FROM test_json_typed
SETTINGS optimize_functions_to_subcolumns = 1;

-- EXPLAIN: chained access on typed dotted path
SELECT 'explain_dotted_typed_chain';
EXPLAIN QUERY TREE run_passes = 1
SELECT json['c']['d'] FROM test_json_dotted
SETTINGS optimize_functions_to_subcolumns = 1;

-- ============================================
-- 8. Equivalence with dot notation
-- ============================================
DROP TABLE IF EXISTS test_json_equiv;
CREATE TABLE test_json_equiv (json JSON) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO test_json_equiv VALUES
('{"a": 1, "b": {"c": 2, "d": {"e": 3}}}'),
('{"a": 10, "b": {"c": 20}}');

SELECT 'equivalence_single';
SELECT
    json['a'] AS bracket, json.a AS dot,
    bracket = dot AS eq
FROM test_json_equiv;

SELECT 'equivalence_nested';
SELECT
    json['b']['c'] AS bracket, json.b.c AS dot,
    bracket = dot AS eq
FROM test_json_equiv;

-- ============================================
-- 9. Edge cases
-- ============================================

-- Empty JSON
DROP TABLE IF EXISTS test_json_empty;
CREATE TABLE test_json_empty (json JSON) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO test_json_empty VALUES ('{}');
SELECT 'empty_json';
SELECT json['a'] FROM test_json_empty;

-- WHERE clause usage
SELECT 'where_clause';
SELECT json['a'] FROM test_json WHERE json['a'] = 1;

-- Multiple JSON columns
DROP TABLE IF EXISTS test_json_multi;
CREATE TABLE test_json_multi (j1 JSON, j2 JSON) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO test_json_multi VALUES ('{"x": 1}', '{"y": 2}');

SELECT 'multi_json';
SELECT j1['x'], j2['y'] FROM test_json_multi;

-- Wrong number of arguments for JSON (3-argument form is only for arrays)
SELECT arrayElement(json, 'a', 0) FROM test_json; -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- ============================================
-- Cleanup
-- ============================================
DROP TABLE test_json;
DROP TABLE test_json_typed;
DROP TABLE test_json_dotted;
DROP TABLE test_json_equiv;
DROP TABLE test_json_empty;
DROP TABLE test_json_multi;
