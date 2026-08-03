
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
SETTINGS auto_statistics_types = 'basic';

SET materialize_statistics_on_insert = 1;

INSERT INTO test_col_stats_agg VALUES
    (1, 10, 5, 100.0, 100.0, NULL, 100.0, 1),
    (2, 20, 3, 200.0, 200.0, NULL, 200.0, 1),
    (3, 5, 2, 50.0, 50.0, NULL, 50.0, 1),
    (4, 30, 10, 300.0, 300.0, NULL, 300.0, 2),
    (5, 15, 4, 150.0, 150.0, NULL, 150.0, 2),
    (6, 2, 3, 25.0, NULL, NULL, 25.0, 3),
    (7, 40, 10, 400.0, 400.0, NULL, inf, 3);

-- Suppress CI setting randomization to ensure deterministic test behavior.
-- Setting randomization may enable/disable optimizations or change parallelism,
-- which can break EXPLAIN output assertions that check for specific plan nodes.
SET explain_query_plan_default = 'legacy';
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
-- Nullable column fallback
-- ==================================================

-- Nullable column: not supported for statistics-based aggregation because the
-- statistics min/max values may differ from the actual `min`/`max` aggregation
-- result when NULLs are present. The query falls back to ReadFromMergeTree.
SELECT trimLeft(explain) FROM (EXPLAIN SELECT min(value_with_null), max(value_with_null) FROM test_col_stats_agg) WHERE explain LIKE '%ReadFromMergeTree%';
SELECT min(value_with_null), max(value_with_null) FROM test_col_stats_agg;

-- Nullable column where ALL values are NULL: statistics have no min/max,
-- so the query falls back to ReadFromMergeTree.
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

-- ==================================================
-- Duplicate aggregate outputs
-- ==================================================

-- Same aggregate without alias - both outputs must produce the same value
SELECT trimLeft(explain) FROM (EXPLAIN SELECT min(value), min(value) FROM test_col_stats_agg) WHERE explain LIKE '%ReadFromPreparedSource%';
SELECT min(value), min(value) FROM test_col_stats_agg;

-- Same aggregate with different aliases - both outputs must produce the same value
SELECT trimLeft(explain) FROM (EXPLAIN SELECT min(value) AS x, min(value) AS y FROM test_col_stats_agg) WHERE explain LIKE '%ReadFromPreparedSource%';
SELECT min(value) AS x, min(value) AS y FROM test_col_stats_agg;

-- Mix of duplicate and unique aggregates
SELECT trimLeft(explain) FROM (EXPLAIN SELECT min(a), min(a), max(a) FROM test_col_stats_agg) WHERE explain LIKE '%ReadFromPreparedSource%';
SELECT min(a), min(a), max(a) FROM test_col_stats_agg;

-- Duplicate aggregates with GROUP BY partition key
SELECT trimLeft(explain) FROM (EXPLAIN SELECT p, min(value), min(value) FROM test_col_stats_agg GROUP BY p ORDER BY p) WHERE explain LIKE '%ReadFromPreparedSource%';
SELECT p, min(value), min(value) FROM test_col_stats_agg GROUP BY p ORDER BY p;

-- ==================================================
-- Duplicate aliases on different source columns
-- ==================================================

-- Different source columns with different aliases: each output must keep its
-- own value (regression: previously the per-position keying was missing,
-- causing aggregates on different columns to collide).
SELECT trimLeft(explain) FROM (EXPLAIN SELECT min(a) AS x, min(b) AS y FROM test_col_stats_agg) WHERE explain LIKE '%ReadFromPreparedSource%';
SELECT min(a) AS x, min(b) AS y FROM test_col_stats_agg;

-- Different columns, mixed min/max
SELECT trimLeft(explain) FROM (EXPLAIN SELECT min(a) AS x, max(b) AS y FROM test_col_stats_agg) WHERE explain LIKE '%ReadFromPreparedSource%';
SELECT min(a) AS x, max(b) AS y FROM test_col_stats_agg;

-- Three aggregates on three different columns
SELECT trimLeft(explain) FROM (EXPLAIN SELECT min(a) AS x, max(value) AS y, min(b) AS z FROM test_col_stats_agg) WHERE explain LIKE '%ReadFromPreparedSource%';
SELECT min(a) AS x, max(value) AS y, min(b) AS z FROM test_col_stats_agg;

-- Different columns with GROUP BY partition key
SELECT trimLeft(explain) FROM (EXPLAIN SELECT p, min(a) AS x, min(b) AS y FROM test_col_stats_agg GROUP BY p ORDER BY p) WHERE explain LIKE '%ReadFromPreparedSource%';
SELECT p, min(a) AS x, min(b) AS y FROM test_col_stats_agg GROUP BY p ORDER BY p;

-- ==================================================
-- GROUP BY key / aggregate tricky output names
-- ==================================================

-- A GROUP BY key alias that textually equals an aggregate output name
-- (GROUP BY p AS "min(a)" together with min(a)): the alias is applied by a
-- projection above the AggregatingStep and never reaches the aggregation keys,
-- so the query can still use column statistics and must stay correct.
SELECT trimLeft(explain) FROM (EXPLAIN SELECT "min(a)", min(a) FROM test_col_stats_agg GROUP BY p AS "min(a)" ORDER BY 1) WHERE explain LIKE '%ReadFromPreparedSource%';
SELECT "min(a)", min(a) FROM test_col_stats_agg GROUP BY p AS "min(a)" ORDER BY 1;

-- Same with several aggregates
SELECT trimLeft(explain) FROM (EXPLAIN SELECT "min(a)", min(a), max(a) FROM test_col_stats_agg GROUP BY p AS "min(a)" ORDER BY 1) WHERE explain LIKE '%ReadFromPreparedSource%';
SELECT "min(a)", min(a), max(a) FROM test_col_stats_agg GROUP BY p AS "min(a)" ORDER BY 1;

DROP TABLE test_col_stats_agg;
