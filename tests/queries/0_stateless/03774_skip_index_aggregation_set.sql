
DROP TABLE IF EXISTS test_skip_index_set_agg;

CREATE TABLE test_skip_index_set_agg (
    id UInt64,
    value Int32,
    overflow_value Int32,
    p String,
    INDEX skip_set_value (value) TYPE set(100) GRANULARITY 1,
    INDEX skip_set_overflow (overflow_value) TYPE set(2) GRANULARITY 1
) ENGINE = MergeTree
PARTITION BY p
ORDER BY id;

INSERT INTO test_skip_index_set_agg VALUES
    (1, 10, 10, 'p1'),
    (2, 20, 20, 'p1'),
    (3, 5, 5, 'p1'),
    (4, 30, 30, 'p2'),
    (5, 15, 15, 'p2'),
    (6, 2, 2, 'p3'),
    (7, 40, 40, 'p3');

SET optimize_use_skip_index_aggregation = 1;
SET optimize_use_projections = 1;
SET optimize_use_implicit_projections = 1;
SET parallel_replicas_local_plan = 1;
SET optimize_aggregation_in_order = 0;
SET count_distinct_optimization = false;

-- ==================================================
-- Basic set index tests for uniq functions
-- ==================================================

-- Test uniq without filter - should use skip index
SELECT trimLeft(explain) FROM (EXPLAIN SELECT uniq(value) FROM test_skip_index_set_agg) WHERE explain LIKE '%ReadFromPreparedSource%';
SELECT uniq(value) FROM test_skip_index_set_agg;

-- Test uniqExact without filter - should use skip index
SELECT trimLeft(explain) FROM (EXPLAIN SELECT uniqExact(value) FROM test_skip_index_set_agg) WHERE explain LIKE '%ReadFromPreparedSource%';
SELECT uniqExact(value) FROM test_skip_index_set_agg;

-- Test uniqCombined without filter - should use skip index
SELECT trimLeft(explain) FROM (EXPLAIN SELECT uniqCombined(value) FROM test_skip_index_set_agg) WHERE explain LIKE '%ReadFromPreparedSource%';
SELECT uniqCombined(value) FROM test_skip_index_set_agg;

-- Test uniqCombined64 without filter - should use skip index
SELECT trimLeft(explain) FROM (EXPLAIN SELECT uniqCombined64(value) FROM test_skip_index_set_agg) WHERE explain LIKE '%ReadFromPreparedSource%';
SELECT uniqCombined64(value) FROM test_skip_index_set_agg;

-- Test uniqHLL12 without filter - should use skip index
SELECT trimLeft(explain) FROM (EXPLAIN SELECT uniqHLL12(value) FROM test_skip_index_set_agg) WHERE explain LIKE '%ReadFromPreparedSource%';
SELECT uniqHLL12(value) FROM test_skip_index_set_agg;

-- ==================================================
-- Parameterized uniq variants (regression guard)
-- ==================================================

-- Test uniqCombined with parameter - should use skip index
SELECT trimLeft(explain) FROM (EXPLAIN SELECT uniqCombined(15)(value) FROM test_skip_index_set_agg) WHERE explain LIKE '%ReadFromPreparedSource%';
SELECT uniqCombined(15)(value) FROM test_skip_index_set_agg;

-- Test uniqUpTo with parameter - should use skip index
-- uniqUpTo without parameter throws, so parameter forwarding must be correct
SELECT trimLeft(explain) FROM (EXPLAIN SELECT uniqUpTo(3)(value) FROM test_skip_index_set_agg) WHERE explain LIKE '%ReadFromPreparedSource%';
SELECT uniqUpTo(3)(value) FROM test_skip_index_set_agg;

-- Test with non-partition filter (primary key) - should NOT use skip index
SELECT trimLeft(explain) FROM (EXPLAIN SELECT uniqExact(value) FROM test_skip_index_set_agg WHERE id < 3) WHERE explain LIKE '%ReadFromMergeTree%';
SELECT uniqExact(value) FROM test_skip_index_set_agg WHERE id < 3;

-- ==================================================
-- GROUP BY partition key tests
-- ==================================================

-- Test GROUP BY partition key - should use skip index
SELECT trimLeft(explain) FROM (EXPLAIN SELECT p, uniqExact(value) FROM test_skip_index_set_agg GROUP BY p ORDER BY p) WHERE explain LIKE '%ReadFromPreparedSource%';
SELECT p, uniqExact(value) FROM test_skip_index_set_agg GROUP BY p ORDER BY p;

-- Test GROUP BY partition key with filter - should use skip index
SELECT trimLeft(explain) FROM (EXPLAIN SELECT p, uniqExact(value) FROM test_skip_index_set_agg WHERE p IN ('p1', 'p2') GROUP BY p ORDER BY p) WHERE explain LIKE '%ReadFromPreparedSource%';
SELECT p, uniqExact(value) FROM test_skip_index_set_agg WHERE p IN ('p1', 'p2') GROUP BY p ORDER BY p;

-- ==================================================
-- Setting disabled fallback test
-- ==================================================

-- Disable skip index aggregation - should fall back to ReadFromMergeTree
SET optimize_use_skip_index_aggregation = 0;
SELECT trimLeft(explain) FROM (EXPLAIN SELECT uniqExact(value) FROM test_skip_index_set_agg) WHERE explain LIKE '%ReadFromMergeTree%';
SELECT uniqExact(value) FROM test_skip_index_set_agg;

-- Re-enable
SET optimize_use_skip_index_aggregation = 1;

-- ==================================================
-- Unmaterialized set index fallback
-- ==================================================

-- Add an unmaterialized set index on another column. Existing parts do not have index data,
-- so the optimizer must fall back when this index would otherwise match the query.
ALTER TABLE test_skip_index_set_agg ADD INDEX idx_set_extra (id) TYPE set(100) GRANULARITY 1;
SELECT trimLeft(explain) FROM (EXPLAIN SELECT uniqExact(id) FROM test_skip_index_set_agg) WHERE explain LIKE '%ReadFromMergeTree%';
SELECT uniqExact(id) FROM test_skip_index_set_agg;

-- ==================================================
-- Multi-argument uniq must NOT be optimized by single-column set index
-- ==================================================

-- uniq(value, id) has 2 arguments, should fall back to ReadFromMergeTree
SELECT trimLeft(explain) FROM (EXPLAIN SELECT uniq(value, id) FROM test_skip_index_set_agg) WHERE explain LIKE '%ReadFromMergeTree%';
SELECT uniq(value, id) FROM test_skip_index_set_agg;

-- ==================================================
-- set index only supports uniq* functions, not min/max/count/sum/avg
-- ==================================================

-- Test avg() with set index - should fall back (set only supports uniq*)
SELECT trimLeft(explain) FROM (EXPLAIN SELECT avg(value) FROM test_skip_index_set_agg) WHERE explain LIKE '%ReadFromMergeTree%';
SELECT avg(value) FROM test_skip_index_set_agg;

-- But uniq* should still work with set index
SELECT trimLeft(explain) FROM (EXPLAIN SELECT uniq(value) FROM test_skip_index_set_agg) WHERE explain LIKE '%ReadFromPreparedSource%';
SELECT uniq(value) FROM test_skip_index_set_agg;

-- ==================================================
-- Set index overflow test
-- ==================================================

-- The overflow_value column has a set(2) index but 3+ distinct values per partition,
-- so the granule overflows. Should NOT use skip index and fall back to ReadFromMergeTree.
SELECT trimLeft(explain) FROM (EXPLAIN SELECT uniqExact(overflow_value) FROM test_skip_index_set_agg) WHERE explain LIKE '%ReadFromMergeTree%';
SELECT uniqExact(overflow_value) FROM test_skip_index_set_agg;

DROP TABLE test_skip_index_set_agg;
