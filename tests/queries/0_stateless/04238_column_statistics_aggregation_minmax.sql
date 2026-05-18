
DROP TABLE IF EXISTS test_col_stats_agg;

CREATE TABLE test_col_stats_agg (
    id UInt64,
    a Int32,
    b Int32,
    value Float64,
    value_with_null Nullable(Float64),
    value_all_null Nullable(Float64),
    value_with_inf Float64,
    p Int32
) ENGINE = MergeTree
PARTITION BY p
ORDER BY id
SETTINGS auto_statistics_types = 'minmax';

SET materialize_statistics_on_insert = 1;

INSERT INTO test_col_stats_agg VALUES
    (1, 10, 5, 100.0, 100.0, NULL, 100.0, 1),
    (2, 20, 3, 200.0, 200.0, NULL, 200.0, 1),
    (3, 5, 2, 50.0, 50.0, NULL, 50.0, 1),
    (4, 30, 10, 300.0, 300.0, NULL, 300.0, 2),
    (5, 15, 4, 150.0, 150.0, NULL, 150.0, 2),
    (6, 2, 3, 25.0, NULL, NULL, 25.0, 3),
    (7, 40, 10, 400.0, 400.0, NULL, inf, 3);

SET optimize_use_projections = 1;
SET optimize_use_implicit_projections = 1;
SET parallel_replicas_local_plan = 1;
SET optimize_aggregation_in_order = 0;

-- ==================================================
-- Basic column statistics aggregation tests
-- ==================================================

-- Test No filter - should use column statistics
SELECT trimLeft(explain) FROM (EXPLAIN SELECT min(value), max(value) FROM test_col_stats_agg) WHERE explain LIKE '%ReadFromPreparedSource%';
SELECT min(value), max(value) FROM test_col_stats_agg;

-- Test Partition filter - should use column statistics
SELECT trimLeft(explain) FROM (EXPLAIN SELECT min(value), max(value) FROM test_col_stats_agg WHERE p = 1) WHERE explain LIKE '%ReadFromPreparedSource%';
SELECT min(value), max(value) FROM test_col_stats_agg WHERE p = 1;

-- Test Non-partition filter (primary key) - should NOT use column statistics
SELECT trimLeft(explain) FROM (EXPLAIN SELECT min(value), max(value) FROM test_col_stats_agg WHERE id < 3) WHERE explain LIKE '%ReadFromMergeTree%';
SELECT min(value), max(value) FROM test_col_stats_agg WHERE id < 3;

-- Test Virtual Column filter - should use column statistics
SELECT trimLeft(explain) FROM (EXPLAIN SELECT min(value), max(value) FROM test_col_stats_agg WHERE _partition_id = '1') WHERE explain LIKE '%ReadFromPreparedSource%';
SELECT min(value), max(value) FROM test_col_stats_agg WHERE _partition_id = '1';

-- Test Integer column - should use column statistics
SELECT trimLeft(explain) FROM (EXPLAIN SELECT min(a), max(a) FROM test_col_stats_agg) WHERE explain LIKE '%ReadFromPreparedSource%';
SELECT min(a), max(a) FROM test_col_stats_agg;

-- ==================================================
-- GROUP BY partition key with column statistics
-- ==================================================

-- Test GROUP BY partition key - should use column statistics
SELECT trimLeft(explain) FROM (EXPLAIN SELECT p, min(value), max(value) FROM test_col_stats_agg GROUP BY p ORDER BY p) WHERE explain LIKE '%ReadFromPreparedSource%';
SELECT p, min(value), max(value) FROM test_col_stats_agg GROUP BY p ORDER BY p;

-- Test GROUP BY partition key with filter - should use column statistics
SELECT trimLeft(explain) FROM (EXPLAIN SELECT p, min(value), max(value) FROM test_col_stats_agg WHERE p IN (1, 2) GROUP BY p ORDER BY p) WHERE explain LIKE '%ReadFromPreparedSource%';
SELECT p, min(value), max(value) FROM test_col_stats_agg WHERE p IN (1, 2) GROUP BY p ORDER BY p;

-- Test GROUP BY non-partition key - should NOT use column statistics
SELECT trimLeft(explain) FROM (EXPLAIN SELECT id, min(value), max(value) FROM test_col_stats_agg GROUP BY id ORDER BY id) WHERE explain LIKE '%ReadFromMergeTree%';
SELECT id, min(value), max(value) FROM test_col_stats_agg GROUP BY id ORDER BY id;

-- Test GROUP BY partition key with multiple aggregate columns - should use column statistics
SELECT trimLeft(explain) FROM (EXPLAIN SELECT p, min(a), max(a), min(b), max(b) FROM test_col_stats_agg GROUP BY p ORDER BY p) WHERE explain LIKE '%ReadFromPreparedSource%';
SELECT p, min(a), max(a), min(b), max(b) FROM test_col_stats_agg GROUP BY p ORDER BY p;

-- ==================================================
-- Nullable column with column statistics
-- ==================================================

-- Nullable column with some NULLs: column statistics stores min/max of non-NULL values,
-- so it should be able to satisfy the aggregation.
SELECT trimLeft(explain) FROM (EXPLAIN SELECT min(value_with_null), max(value_with_null) FROM test_col_stats_agg) WHERE explain LIKE '%ReadFromPreparedSource%';
SELECT min(value_with_null), max(value_with_null) FROM test_col_stats_agg;

-- Nullable column where ALL values are NULL: estimated_min/max will be absent,
-- so column statistics cannot satisfy the query and should fall back.
SELECT trimLeft(explain) FROM (EXPLAIN SELECT min(value_all_null), max(value_all_null) FROM test_col_stats_agg) WHERE explain LIKE '%ReadFromMergeTree%';
SELECT min(value_all_null), max(value_all_null) FROM test_col_stats_agg;

-- Infinity bounds in column statistics are handled at runtime. Verify the final answers stay correct.
SELECT min(value_with_inf), max(value_with_inf) FROM test_col_stats_agg;

-- GROUP BY partition key with an infinity bound must also keep the final answers correct.
SELECT p, min(value_with_inf), max(value_with_inf) FROM test_col_stats_agg GROUP BY p ORDER BY p;

-- ==================================================
-- Mixed unsupported aggregate fallback test
-- ==================================================

-- Mixed supported + unsupported aggregate - should fall back
SELECT trimLeft(explain) FROM (EXPLAIN SELECT min(value), sum(value) FROM test_col_stats_agg) WHERE explain LIKE '%ReadFromMergeTree%';
SELECT min(value), sum(value) FROM test_col_stats_agg;

DROP TABLE test_col_stats_agg;
