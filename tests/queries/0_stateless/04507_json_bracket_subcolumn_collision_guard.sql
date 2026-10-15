-- Regression test: json['key'] subcolumn optimization must not rewrite to a physical
-- column whose name happens to match the generated JSON subcolumn name.
--
-- The JSON subcolumn name for json['a'] is `json.@`a`` (prefix '@' + backquoted key).
-- If a physical column with that exact name exists, `sourceHasColumn` must block the
-- rewrite so that `json['a']` still reads from the JSON column, not the physical one.

SET enable_analyzer = 1;

-- ============================================================
-- 1. No-collision case: optimization should fire normally.
-- ============================================================
SELECT 'no_collision';

DROP TABLE IF EXISTS test_json_no_collision;
CREATE TABLE test_json_no_collision (json JSON) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO test_json_no_collision VALUES ('{"a": 42}'), ('{"a": 100}');

-- Both settings should give the same results.
SELECT json['a'] FROM test_json_no_collision ORDER BY json['a']
SETTINGS optimize_functions_to_subcolumns = 1, allow_suspicious_types_in_order_by = 1;

SELECT json['a'] FROM test_json_no_collision ORDER BY json['a']
SETTINGS optimize_functions_to_subcolumns = 0, allow_suspicious_types_in_order_by = 1;

-- EXPLAIN: optimization applied -> shows COLUMN node, not FUNCTION node.
SELECT 'explain_no_collision_optimized';
EXPLAIN QUERY TREE run_passes = 1
SELECT json['a'] FROM test_json_no_collision
SETTINGS optimize_functions_to_subcolumns = 1;

-- ============================================================
-- 2. Collision case: physical column name == JSON subcolumn name.
--    The optimizer must skip the rewrite; results must still be
--    the JSON field values, not the physical column values.
-- ============================================================
SELECT 'collision';

DROP TABLE IF EXISTS test_json_collision;
CREATE TABLE test_json_collision
(
    id   UInt32,
    json JSON,
    `json.@\`a\`` UInt32
) ENGINE = MergeTree ORDER BY id;

INSERT INTO test_json_collision VALUES (1, '{"a": 42}',  999);
INSERT INTO test_json_collision VALUES (2, '{"a": 100}', 888);

-- json['a'] must return 42 and 100 (from JSON), not 999/888 (from physical column).
SELECT json['a'] FROM test_json_collision ORDER BY id
SETTINGS optimize_functions_to_subcolumns = 1;

SELECT json['a'] FROM test_json_collision ORDER BY id
SETTINGS optimize_functions_to_subcolumns = 0;

-- EXPLAIN with optimization: collision guard blocks the rewrite ->
-- shows FUNCTION node (arrayElement), not COLUMN node.
SELECT 'explain_collision_not_optimized';
EXPLAIN QUERY TREE run_passes = 1
SELECT json['a'] FROM test_json_collision
SETTINGS optimize_functions_to_subcolumns = 1;

-- Cleanup.
DROP TABLE test_json_no_collision;
DROP TABLE test_json_collision;
