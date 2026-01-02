DROP TABLE IF EXISTS test_skip_index_minmax_agg;

CREATE TABLE test_skip_index_minmax_agg (
    id UInt64,
    a Int32,
    b Int32,
    value Float64,
    p Int32,
    INDEX skip_minmax_value (value) TYPE minmax GRANULARITY 1,
    INDEX skip_minmax_expr (a * b) TYPE minmax GRANULARITY 1,
    INDEX skip_minmax_multi (a, b) TYPE minmax GRANULARITY 1
) ENGINE = MergeTree
PARTITION BY p
ORDER BY id;

INSERT INTO test_skip_index_minmax_agg VALUES
    (1, 10, 5, 100.0, 1),
    (2, 20, 3, 200.0, 1),
    (3, 5, 2, 50.0, 1),
    (4, 30, 10, 300.0, 2),
    (5, 15, 4, 150.0, 2),
    (6, 2, 3, 25.0, 3),
    (7, 40, 10, 400.0, 3);

SET allow_skip_index_aggregation_optimize = 1;
-- For parallel replicas
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

DROP TABLE test_skip_index_minmax_agg;
