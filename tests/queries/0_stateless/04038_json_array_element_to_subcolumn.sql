-- Tags: no-fasttest, no-parallel-replicas

SET enable_analyzer = 1;

DROP TABLE IF EXISTS test_json_arr;

CREATE TABLE test_json_arr (json JSON) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO test_json_arr VALUES
('{"a": [{"b": 1, "c": "x"}, {"b": 2, "c": "y"}, {"b": 3, "c": "z"}]}'),
('{"a": [{"b": 10, "c": "p"}, {"b": 20, "c": "q"}]}');

-- 1. Basic correctness: json.a[1].b vs json.a[].b[1]
SELECT 'basic';
SELECT json.a[1].b, json.a[].b[1] FROM test_json_arr ORDER BY toString(json.a[1].b);

-- 2. Multi-level field: json.a[2].c
SELECT 'multi';
SELECT json.a[2].c FROM test_json_arr ORDER BY toString(json.a[2].c);

-- 3. Out of bounds
SELECT 'oob';
SELECT json.a[100].b FROM test_json_arr;

-- 4. Variable index
SELECT 'variable_index';
SELECT number + 1 as idx, json.a[idx].b FROM test_json_arr, numbers(3) AS n ORDER BY toString(json), idx;

-- 5. EXPLAIN showing the optimization is applied: should contain arrayElement with Array(JSON) subcolumn
SELECT 'explain_optimized';
EXPLAIN QUERY TREE run_passes = 1 SELECT json.a[1].b FROM test_json_arr
SETTINGS optimize_functions_to_subcolumns = 1;

-- 6. EXPLAIN without optimization: should contain tupleElement(arrayElement(...))
SELECT 'explain_not_optimized';
EXPLAIN QUERY TREE run_passes = 1 SELECT json.a[1].b FROM test_json_arr
SETTINGS optimize_functions_to_subcolumns = 0;

-- 7. Nested path before array
DROP TABLE IF EXISTS test_json_arr2;
CREATE TABLE test_json_arr2 (json JSON) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO test_json_arr2 VALUES
('{"x": {"y": [{"z": 100}, {"z": 200}]}}');

SELECT 'nested_path';
SELECT json.x.y[1].z FROM test_json_arr2;
SELECT json.x.y[2].z FROM test_json_arr2;

-- 8. Deep field extraction: json.a[1].b.c
DROP TABLE IF EXISTS test_json_arr3;
CREATE TABLE test_json_arr3 (json JSON) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO test_json_arr3 VALUES
('{"a": [{"b": {"c": 42, "d": "hello"}}, {"b": {"c": 99}}]}');

SELECT 'deep_field';
SELECT json.a[1].b.c FROM test_json_arr3;
SELECT json.a[2].b.c FROM test_json_arr3;

-- 9. Mixed usage: should NOT optimize (tupleElement should remain because json.a is also read directly)
SELECT 'mixed_usage';
EXPLAIN QUERY TREE run_passes = 1
SELECT json.a[1].b, json.a FROM test_json_arr
SETTINGS optimize_functions_to_subcolumns = 1;

-- 10. Multiple optimizable usages of same column
SELECT 'multiple_opt';
SELECT json.a[1].b, json.a[2].c FROM test_json_arr ORDER BY toString(json.a[1].b);

-- 11. Tuple(JSON): optimization should find JSON ancestor through Tuple subcolumn
SELECT 'tuple_json';
DROP TABLE IF EXISTS test_json_in_tuple;
CREATE TABLE test_json_in_tuple (t Tuple(json JSON)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO test_json_in_tuple VALUES (('{"a": [{"b": 1}, {"b": 2}]}'));

SELECT t.json.a[1].b FROM test_json_in_tuple;
SELECT t.json.a[2].b FROM test_json_in_tuple;

-- 12. Dotted key names inside JSON (backtick-escaped)
SELECT 'dotted_keys';
DROP TABLE IF EXISTS test_json_dotted;
CREATE TABLE test_json_dotted (json JSON) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO test_json_dotted VALUES ('{"a.b": [{"c.d": 10}, {"c.d": 20}]}');

SELECT json.`a.b`[1].`c.d` FROM test_json_dotted;
SELECT json.`a.b`[2].`c.d` FROM test_json_dotted;

-- 13. Negative index
SELECT 'negative_index';
SELECT json.a[-1].b FROM test_json_arr ORDER BY toString(json.a[-1].b);

-- 14. WHERE clause usage
SELECT 'where_clause';
SELECT json.a[1].b FROM test_json_arr WHERE json.a[1].b = 1;

-- 15. FINAL modifier should block optimization
SELECT 'final_blocked';
DROP TABLE IF EXISTS test_json_final;
CREATE TABLE test_json_final (json JSON) ENGINE = ReplacingMergeTree ORDER BY tuple();
INSERT INTO test_json_final VALUES ('{"a": [{"b": 1}]}');

EXPLAIN QUERY TREE run_passes = 1
SELECT json.a[1].b FROM test_json_final FINAL
SETTINGS optimize_functions_to_subcolumns = 1;

-- 16. Empty array
SELECT 'empty_array';
DROP TABLE IF EXISTS test_json_empty;
CREATE TABLE test_json_empty (json JSON) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO test_json_empty VALUES ('{"a": []}');
INSERT INTO test_json_empty VALUES ('{"x": 1}');

SELECT json.a[1].b FROM test_json_empty;

-- 17. Multiple JSON columns
SELECT 'multi_json_cols';
DROP TABLE IF EXISTS test_multi_json;
CREATE TABLE test_multi_json (j1 JSON, j2 JSON) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO test_multi_json VALUES ('{"a": [{"b": 1}]}', '{"a": [{"b": 100}]}');

SELECT j1.a[1].b, j2.a[1].b FROM test_multi_json;

-- Cleanup
DROP TABLE test_json_arr;
DROP TABLE test_json_arr2;
DROP TABLE test_json_arr3;
DROP TABLE test_json_in_tuple;
DROP TABLE test_json_dotted;
DROP TABLE test_json_final;
DROP TABLE test_json_empty;
DROP TABLE test_multi_json;
