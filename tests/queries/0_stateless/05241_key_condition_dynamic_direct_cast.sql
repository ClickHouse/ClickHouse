-- Tags: no-random-settings, no-random-merge-tree-settings
-- The selected mark counts depend on fixed granularity and index-analysis settings.

SET use_statistics_for_part_pruning = 0;
SET use_query_condition_cache = 0;
SET optimize_use_projections = 0;
SET optimize_trivial_count_query = 0;
SET optimize_trivial_count_with_sparsity_filter = 0;
SET optimize_move_to_prewhere = 0;
SET analyze_index_with_multiple_key_columns_per_condition = 1;

-- A direct `CAST` of a `Dynamic` key can consume the constant's original type without preparing
-- an intermediate `Dynamic` conversion. Aliases on the cast output preserve this property.
DROP TABLE IF EXISTS test_dynamic_cast;
CREATE TABLE test_dynamic_cast (v Dynamic) ENGINE = MergeTree
ORDER BY tuple(CAST(v, 'String') AS text_key)
SETTINGS index_granularity = 1, index_granularity_bytes = 0,
    min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0,
    add_minmax_index_for_numeric_columns = 0;
INSERT INTO test_dynamic_cast SELECT toString(number) FROM numbers(16);

SELECT marks FROM (EXPLAIN ESTIMATE SELECT * FROM test_dynamic_cast WHERE v = '5');
SELECT count() FROM test_dynamic_cast WHERE v = '5' SETTINGS max_rows_to_read = 2;
SELECT marks FROM (EXPLAIN ESTIMATE SELECT * FROM test_dynamic_cast WHERE v = '5' OR v = '6');
SELECT count() FROM test_dynamic_cast WHERE v = '5' OR v = '6' SETTINGS max_rows_to_read = 3;
DROP TABLE test_dynamic_cast;

-- The direct cast also converts a constant whose type differs from the key result type.
DROP TABLE IF EXISTS test_dynamic_cast;
CREATE TABLE test_dynamic_cast (v Dynamic) ENGINE = MergeTree
ORDER BY tuple(CAST(v, 'UInt64') AS numeric_key)
SETTINGS index_granularity = 1, index_granularity_bytes = 0,
    min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0,
    add_minmax_index_for_numeric_columns = 0;
INSERT INTO test_dynamic_cast SELECT toString(number) FROM numbers(16);

SELECT marks FROM (EXPLAIN ESTIMATE SELECT * FROM test_dynamic_cast WHERE v = '5');
SELECT count() FROM test_dynamic_cast WHERE v = '5' SETTINGS max_rows_to_read = 2;
SELECT marks FROM (EXPLAIN ESTIMATE SELECT * FROM test_dynamic_cast WHERE v = '5' OR v = '6');
SELECT count() FROM test_dynamic_cast WHERE v = '5' OR v = '6' SETTINGS max_rows_to_read = 3;
DROP TABLE test_dynamic_cast;

-- A composed cast cannot skip the expression between the input and the output.
DROP TABLE IF EXISTS test_dynamic_cast;
CREATE TABLE test_dynamic_cast (v Dynamic) ENGINE = MergeTree
ORDER BY tuple(CAST(lower(CAST(v, 'String')), 'String') AS composed_key)
SETTINGS index_granularity = 1, index_granularity_bytes = 0,
    min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0,
    add_minmax_index_for_numeric_columns = 0;
INSERT INTO test_dynamic_cast SELECT toString(number) FROM numbers(16);

SELECT marks FROM (EXPLAIN ESTIMATE SELECT * FROM test_dynamic_cast WHERE v = '5');
SELECT count() FROM test_dynamic_cast WHERE v = '5' SETTINGS max_rows_to_read = 16;
SELECT marks FROM (EXPLAIN ESTIMATE SELECT * FROM test_dynamic_cast WHERE v = '5' OR v = '6');
SELECT count() FROM test_dynamic_cast WHERE v = '5' OR v = '6' SETTINGS max_rows_to_read = 16;
DROP TABLE test_dynamic_cast;

-- Other conversion functions do not use the direct-cast equivalence.
DROP TABLE IF EXISTS test_dynamic_cast;
CREATE TABLE test_dynamic_cast (v Dynamic) ENGINE = MergeTree
ORDER BY tuple(toString(v) AS converted_key)
SETTINGS allow_nullable_key = 1, index_granularity = 1, index_granularity_bytes = 0,
    min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0,
    add_minmax_index_for_numeric_columns = 0;
INSERT INTO test_dynamic_cast SELECT toString(number) FROM numbers(16);

SELECT marks FROM (EXPLAIN ESTIMATE SELECT * FROM test_dynamic_cast WHERE v = '5');
SELECT count() FROM test_dynamic_cast WHERE v = '5' SETTINGS max_rows_to_read = 16;
SELECT marks FROM (EXPLAIN ESTIMATE SELECT * FROM test_dynamic_cast WHERE v = '5' OR v = '6');
SELECT count() FROM test_dynamic_cast WHERE v = '5' OR v = '6' SETTINGS max_rows_to_read = 16;
DROP TABLE test_dynamic_cast;
