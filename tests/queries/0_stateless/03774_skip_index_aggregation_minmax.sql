DROP TABLE IF EXISTS test_skip_index_minmax_agg;

CREATE TABLE test_skip_index_minmax_agg (
    id UInt64,
    a Int32,
    b Int32,
    value Float64,
    value_with_null Nullable(Float64),
    value_int Int32,
    p Int32,
    INDEX skip_minmax_value (value) TYPE minmax GRANULARITY 1,
    INDEX skip_minmax_expr (a * b) TYPE minmax GRANULARITY 1,
    INDEX skip_minmax_multi (a, b) TYPE minmax GRANULARITY 1,
    INDEX skip_minmax_nullable (value_with_null) TYPE minmax GRANULARITY 1
) ENGINE = MergeTree
PARTITION BY p
ORDER BY id
SETTINGS add_minmax_index_for_numeric_columns = 0, auto_statistics_types = '';

INSERT INTO test_skip_index_minmax_agg VALUES
    (1, 10, 5, 100.0, 100.0, 10, 1),
    (2, 20, 3, 200.0, 200.0, 20, 1),
    (3, 5, 2, 50.0, 50.0, 5, 1),
    (4, 30, 10, 300.0, 300.0, 30, 2),
    (5, 15, 4, 150.0, 150.0, 15, 2),
    (6, 2, 3, 25.0, NULL, 2, 3),
    (7, 40, 10, 400.0, 400.0, 40, 3);

SET optimize_use_skip_index_aggregation = 1;
SET parallel_replicas_local_plan = 1;
SET optimize_aggregation_in_order = 0;

-- ==================================================
-- Basic single-column minmax index tests
-- ==================================================

-- Test No filter - should use skip index
SELECT trimLeft(explain) FROM (EXPLAIN SELECT min(value), max(value) FROM test_skip_index_minmax_agg) WHERE explain LIKE '%ReadFromPreparedSource%';
SELECT min(value), max(value) FROM test_skip_index_minmax_agg;

-- Test Partition filter - should use skip index
SELECT trimLeft(explain) FROM (EXPLAIN SELECT min(value), max(value) FROM test_skip_index_minmax_agg WHERE p = 1) WHERE explain LIKE '%ReadFromPreparedSource%';
SELECT min(value), max(value) FROM test_skip_index_minmax_agg WHERE p = 1;

-- Test Non-partition filter (primary key) - should NOT use skip index
SELECT trimLeft(explain) FROM (EXPLAIN SELECT min(value), max(value) FROM test_skip_index_minmax_agg WHERE id < 3) WHERE explain LIKE '%ReadFromMergeTree%';
SELECT min(value), max(value) FROM test_skip_index_minmax_agg WHERE id < 3;

-- Test Virtual Column filter - should use skip index
SELECT trimLeft(explain) FROM (EXPLAIN SELECT min(value), max(value) FROM test_skip_index_minmax_agg WHERE _partition_id = '1') WHERE explain LIKE '%ReadFromPreparedSource%';
SELECT min(value), max(value) FROM test_skip_index_minmax_agg WHERE _partition_id = '1';

-- ==================================================
-- Expression-based minmax index tests
-- ==================================================

-- Test Expression index without filter - should use skip index
SELECT trimLeft(explain) FROM (EXPLAIN SELECT min(a * b), max(a * b) FROM test_skip_index_minmax_agg) WHERE explain LIKE '%ReadFromPreparedSource%';
SELECT min(a * b), max(a * b) FROM test_skip_index_minmax_agg;

-- Test Expression index with partition filter - should use skip index
SELECT trimLeft(explain) FROM (EXPLAIN SELECT min(a * b), max(a * b) FROM test_skip_index_minmax_agg WHERE p = 1) WHERE explain LIKE '%ReadFromPreparedSource%';
SELECT min(a * b), max(a * b) FROM test_skip_index_minmax_agg WHERE p = 1;

-- ==================================================
-- Multi-column minmax index tests
-- ==================================================

-- Test both columns - should use skip index
SELECT trimLeft(explain) FROM (EXPLAIN SELECT min(a), max(a), min(b), max(b) FROM test_skip_index_minmax_agg) WHERE explain LIKE '%ReadFromPreparedSource%';
SELECT min(a), max(a), min(b), max(b) FROM test_skip_index_minmax_agg;

-- ==================================================
-- GROUP BY partition key tests
-- ==================================================

-- Test GROUP BY partition key - should use skip index
SELECT trimLeft(explain) FROM (EXPLAIN SELECT p, min(value), max(value) FROM test_skip_index_minmax_agg GROUP BY p ORDER BY p) WHERE explain LIKE '%ReadFromPreparedSource%';
SELECT p, min(value), max(value) FROM test_skip_index_minmax_agg GROUP BY p ORDER BY p;

-- Test GROUP BY partition key with partition key filter - should use skip index
SELECT trimLeft(explain) FROM (EXPLAIN SELECT p, min(value), max(value) FROM test_skip_index_minmax_agg WHERE p IN (1, 2) GROUP BY p ORDER BY p) WHERE explain LIKE '%ReadFromPreparedSource%';
SELECT p, min(value), max(value) FROM test_skip_index_minmax_agg WHERE p IN (1, 2) GROUP BY p ORDER BY p;

-- Test GROUP BY non-partition key - should NOT use skip index
SELECT trimLeft(explain) FROM (EXPLAIN SELECT id, min(value), max(value) FROM test_skip_index_minmax_agg GROUP BY id ORDER BY id) WHERE explain LIKE '%ReadFromMergeTree%';
SELECT id, min(value), max(value) FROM test_skip_index_minmax_agg GROUP BY id ORDER BY id;

-- Test GROUP BY partition key with multi-column index
SELECT trimLeft(explain) FROM (EXPLAIN SELECT p, min(a), max(b) FROM test_skip_index_minmax_agg GROUP BY p ORDER BY p) WHERE explain LIKE '%ReadFromPreparedSource%';
SELECT p, min(a), max(b) FROM test_skip_index_minmax_agg GROUP BY p ORDER BY p;

-- ==================================================
-- Setting disabled fallback tests
-- ==================================================

-- Disable skip index aggregation - should fall back to ReadFromMergeTree
SET optimize_use_skip_index_aggregation = 0;
SELECT trimLeft(explain) FROM (EXPLAIN SELECT min(value), max(value) FROM test_skip_index_minmax_agg) WHERE explain LIKE '%ReadFromMergeTree%';
SELECT min(value), max(value) FROM test_skip_index_minmax_agg;

-- Re-enable for subsequent tests
SET optimize_use_skip_index_aggregation = 1;

-- ==================================================
-- Unmaterialized index fallback test
-- ==================================================

-- Add index without materializing - index data does not exist in existing parts
ALTER TABLE test_skip_index_minmax_agg ADD INDEX idx_minmax (value) TYPE minmax GRANULARITY 1;

-- Should fall back to ReadFromMergeTree because index is not materialized
SELECT trimLeft(explain) FROM (EXPLAIN SELECT min(value), max(value) FROM test_skip_index_minmax_agg) WHERE explain LIKE '%ReadFromMergeTree%';
SELECT min(value), max(value) FROM test_skip_index_minmax_agg;

-- ==================================================
-- Mixed unsupported aggregate fallback test
-- ==================================================

-- Mixed min/max + sum - cannot be satisfied by skip index, should fall back
SELECT trimLeft(explain) FROM (EXPLAIN SELECT min(value), sum(value) FROM test_skip_index_minmax_agg) WHERE explain LIKE '%ReadFromMergeTree%';
SELECT min(value), sum(value) FROM test_skip_index_minmax_agg;

-- ==================================================
-- Nullable column tests
-- ==================================================

-- Nullable columns with NULLs: if granules contain NULL data, the hyperrectangle has NULL bounds.
-- The optimizer detects this at runtime and falls back to ReadFromMergeTree for correctness.
SELECT trimLeft(explain) FROM (EXPLAIN SELECT min(value_with_null), max(value_with_null) FROM test_skip_index_minmax_agg) WHERE explain LIKE '%ReadFromMergeTree%';
SELECT min(value_with_null), max(value_with_null) FROM test_skip_index_minmax_agg;

DROP TABLE test_skip_index_minmax_agg;

-- ==================================================
-- Column Statistics (auto_statistics_types) tests
-- ==================================================

-- Create a table with auto minmax statistics but no explicit skip indices.
-- Column statistics MinMax is stored per-part (not per-granule), so it does not
-- have the same NULL-sensitivity as skip index hyperrectangles.

DROP TABLE IF EXISTS test_col_stats_agg;
CREATE TABLE test_col_stats_agg (
    id UInt64,
    a Int32,
    b Int32,
    value Float64,
    value_with_null Nullable(Float64),
    value_all_null Nullable(Float64),
    p Int32
) ENGINE = MergeTree
PARTITION BY p
ORDER BY id
SETTINGS add_minmax_index_for_numeric_columns = 0, auto_statistics_types = 'minmax';

INSERT INTO test_col_stats_agg VALUES
    (1, 10, 5, 100.0, 100.0, NULL, 1),
    (2, 20, 3, 200.0, 200.0, NULL, 1),
    (3, 5, 2, 50.0, 50.0, NULL, 1),
    (4, 30, 10, 300.0, 300.0, NULL, 2),
    (5, 15, 4, 150.0, 150.0, NULL, 2),
    (6, 2, 3, 25.0, NULL, NULL, 3),
    (7, 40, 10, 400.0, 400.0, NULL, 3);

-- Force statistics to be recomputed for existing parts
ALTER TABLE test_col_stats_agg MATERIALIZE STATISTICS ALL;

SET optimize_use_skip_index_aggregation = 1;
SET parallel_replicas_local_plan = 1;
SET optimize_aggregation_in_order = 0;

-- ==================================================
-- Basic column statistics aggregation tests
-- ==================================================

-- Test No filter - should use column statistics (_column_statistics_aggregation)
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

-- Test GROUP BY partition key with multiple aggregate columns
SELECT trimLeft(explain) FROM (EXPLAIN SELECT p, min(a), max(a), min(b), max(b) FROM test_col_stats_agg GROUP BY p ORDER BY p) WHERE explain LIKE '%ReadFromPreparedSource%';
SELECT p, min(a), max(a), min(b), max(b) FROM test_col_stats_agg GROUP BY p ORDER BY p;

-- ==================================================
-- Nullable column with column statistics
-- ==================================================

-- Nullable column with some NULLs: column statistics stores min/max of non-NULL values,
-- so it should be able to satisfy the aggregation (unlike skip index which falls back).
SELECT trimLeft(explain) FROM (EXPLAIN SELECT min(value_with_null), max(value_with_null) FROM test_col_stats_agg) WHERE explain LIKE '%ReadFromPreparedSource%';
SELECT min(value_with_null), max(value_with_null) FROM test_col_stats_agg;

-- Nullable column where ALL values are NULL: estimated_min/max will be absent,
-- so column statistics cannot satisfy the query and should fall back.
SELECT trimLeft(explain) FROM (EXPLAIN SELECT min(value_all_null), max(value_all_null) FROM test_col_stats_agg) WHERE explain LIKE '%ReadFromMergeTree%';
SELECT min(value_all_null), max(value_all_null) FROM test_col_stats_agg;

-- ==================================================
-- Column statistics priority over skip index
-- ==================================================

-- Create a table with BOTH column statistics and a skip index on the same column.
-- Column statistics should take priority (checked before skip index in the optimizer).
DROP TABLE IF EXISTS test_col_stats_and_skip_index;
CREATE TABLE test_col_stats_and_skip_index (
    id UInt64,
    value Float64,
    p Int32,
    INDEX skip_minmax_value (value) TYPE minmax GRANULARITY 1
) ENGINE = MergeTree
PARTITION BY p
ORDER BY id
SETTINGS add_minmax_index_for_numeric_columns = 0, auto_statistics_types = 'minmax';

INSERT INTO test_col_stats_and_skip_index VALUES
    (1, 100.0, 1),
    (2, 200.0, 1),
    (3, 300.0, 2),
    (4, 400.0, 2);

ALTER TABLE test_col_stats_and_skip_index MATERIALIZE STATISTICS ALL;

-- Should use column statistics (shown as _column_statistics_aggregation), not skip index
SELECT trimLeft(explain) FROM (EXPLAIN SELECT min(value), max(value) FROM test_col_stats_and_skip_index) WHERE explain LIKE '%ReadFromPreparedSource%';
SELECT min(value), max(value) FROM test_col_stats_and_skip_index;

DROP TABLE test_col_stats_and_skip_index;

-- ==================================================
-- Setting disabled fallback with column statistics
-- ==================================================

-- Disable skip index aggregation - column statistics should also be disabled by this setting
SET optimize_use_skip_index_aggregation = 0;
SELECT trimLeft(explain) FROM (EXPLAIN SELECT min(value), max(value) FROM test_col_stats_agg) WHERE explain LIKE '%ReadFromMergeTree%';
SELECT min(value), max(value) FROM test_col_stats_agg;

-- Re-enable for subsequent tests
SET optimize_use_skip_index_aggregation = 1;

-- ==================================================
-- Column without MinMax statistics fallback
-- ==================================================

-- value_all_null column has MinMax statistics but no actual min/max data (all NULLs).
-- A column without any MinMax statistics configured should also fall back.

-- Verify: aggregate on a column that has MinMax stats (value) works
SELECT trimLeft(explain) FROM (EXPLAIN SELECT min(value), max(value) FROM test_col_stats_agg) WHERE explain LIKE '%ReadFromPreparedSource%';
SELECT min(value), max(value) FROM test_col_stats_agg;

-- Mixed supported + unsupported aggregate - should fall back
SELECT trimLeft(explain) FROM (EXPLAIN SELECT min(value), sum(value) FROM test_col_stats_agg) WHERE explain LIKE '%ReadFromMergeTree%';
SELECT min(value), sum(value) FROM test_col_stats_agg;

DROP TABLE test_col_stats_agg;
