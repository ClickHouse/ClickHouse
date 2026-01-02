DROP TABLE IF EXISTS test_skip_index_set_agg;

CREATE TABLE test_skip_index_set_agg (
    id UInt64,
    value Int32,
    p String,
    INDEX skip_set_value (value) TYPE set(100) GRANULARITY 1
) ENGINE = MergeTree
PARTITION BY p
ORDER BY id;

INSERT INTO test_skip_index_set_agg VALUES
    (1, 10, 'p1'),
    (2, 20, 'p1'),
    (3, 5, 'p1'),
    (4, 30, 'p2'),
    (5, 15, 'p2'),
    (6, 2, 'p3'),
    (7, 40, 'p3');

SET allow_skip_index_aggregation_optimize = 1;
-- For parallel replicas
SET parallel_replicas_local_plan = 1;
SET optimize_aggregation_in_order = 0;

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
-- Set index overflow test
-- ==================================================

DROP TABLE IF EXISTS test_skip_index_set_overflow;

CREATE TABLE test_skip_index_set_overflow (
    id UInt64,
    value Int32,
    INDEX skip_set_value (value) TYPE set(2) GRANULARITY 1  -- max_rows=2, will overflow
) ENGINE = MergeTree
ORDER BY id;

-- Insert more than 2 unique values in a single granule to trigger overflow
INSERT INTO test_skip_index_set_overflow VALUES (1, 10), (2, 20), (3, 30), (4, 40);

-- Should NOT use skip index due to overflow
SELECT trimLeft(explain) FROM (EXPLAIN SELECT uniqExact(value) FROM test_skip_index_set_overflow) WHERE explain LIKE '%ReadFromMergeTree%';
SELECT uniqExact(value) FROM test_skip_index_set_overflow;

DROP TABLE test_skip_index_set_overflow;
DROP TABLE test_skip_index_set_agg;
