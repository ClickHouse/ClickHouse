-- Tags: no-random-settings, no-random-merge-tree-settings
-- The selected mark counts depend on the index granularity and index-analysis settings.

SET use_statistics_for_part_pruning = 0;
SET use_query_condition_cache = 0;
SET optimize_use_projections = 0;
SET optimize_trivial_count_query = 0;
SET optimize_trivial_count_with_sparsity_filter = 0;

DROP TABLE IF EXISTS test_nested_nan_sets;
CREATE TABLE test_nested_nan_sets
(
    k Tuple(Float64, Array(Float64), Tuple(Float64), Map(String, Float64))
) ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 1, index_granularity_bytes = 0,
    min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0,
    add_minmax_index_for_numeric_columns = 0;

INSERT INTO test_nested_nan_sets
SELECT tuple(toFloat64(number), [toFloat64(number)], tuple(toFloat64(number)), map('x', toFloat64(number)))
FROM numbers(8);
INSERT INTO test_nested_nan_sets VALUES
    ((nan, [1.], (1.), {'x': 1.})),
    ((1., [nan], (1.), {'x': 1.})),
    ((1., [1.], (nan), {'x': 1.})),
    ((1., [1.], (1.), {'x': nan}));
OPTIMIZE TABLE test_nested_nan_sets FINAL;

-- A NaN anywhere in a set value makes its range atom unavailable, including inside maps.
-- Each predicate still matches its row, and none relies on NaN-free index bounds for pruning.
SELECT count() FROM test_nested_nan_sets WHERE k IN (tuple(nan, [1.], tuple(1.), map('x', 1.)));
SELECT marks FROM (EXPLAIN ESTIMATE SELECT * FROM test_nested_nan_sets WHERE k IN (tuple(nan, [1.], tuple(1.), map('x', 1.))));
SELECT count() FROM test_nested_nan_sets WHERE k IN (tuple(1., [nan], tuple(1.), map('x', 1.)));
SELECT marks FROM (EXPLAIN ESTIMATE SELECT * FROM test_nested_nan_sets WHERE k IN (tuple(1., [nan], tuple(1.), map('x', 1.))));
SELECT count() FROM test_nested_nan_sets WHERE k IN (tuple(1., [1.], tuple(nan), map('x', 1.)));
SELECT marks FROM (EXPLAIN ESTIMATE SELECT * FROM test_nested_nan_sets WHERE k IN (tuple(1., [1.], tuple(nan), map('x', 1.))));
SELECT count() FROM test_nested_nan_sets WHERE k IN (tuple(1., [1.], tuple(1.), map('x', nan)));
SELECT marks FROM (EXPLAIN ESTIMATE SELECT * FROM test_nested_nan_sets WHERE k IN (tuple(1., [1.], tuple(1.), map('x', nan))));

-- A finite set keeps pruning, even when other stored values contain NaNs.
SELECT count() FROM test_nested_nan_sets WHERE k IN (tuple(5., [5.], tuple(5.), map('x', 5.)))
SETTINGS force_primary_key = 1, max_rows_to_read = 2;
SELECT marks FROM (EXPLAIN ESTIMATE SELECT * FROM test_nested_nan_sets WHERE k IN (tuple(5., [5.], tuple(5.), map('x', 5.))));

DROP TABLE test_nested_nan_sets;
