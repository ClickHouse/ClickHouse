
DROP TABLE IF EXISTS test_skip_index_minmax;

CREATE TABLE test_skip_index_minmax (
    id UInt64,
    value Float64,
    partition_key Int32,
    INDEX skip_minmax_value value TYPE minmax GRANULARITY 1
) ENGINE = MergeTree
PARTITION BY partition_key
ORDER BY id;

INSERT INTO test_skip_index_minmax VALUES
    (1, 100.0, 1),
    (2, 200.0, 1),
    (3, 50.0, 1),
    (4, 300.0, 2),
    (5, 150.0, 2),
    (6, 25.0, 3),
    (7, 400.0, 3);

SET parallel_replicas_local_plan = 1, optimize_aggregation_in_order = 0;

-- Test: with non filter should use skip index
SELECT trimLeft(*) FROM (EXPLAIN SELECT min(value), max(value) FROM test_skip_index_minmax) where explain like '%ReadFromPreparedSource%';
SELECT min(value), max(value) FROM test_skip_index_minmax;

-- Test: with partition filter should use skip index
SELECT trimLeft(*) FROM (EXPLAIN SELECT min(value), max(value) FROM test_skip_index_minmax WHERE partition_key = 1) where explain like '%ReadFromPreparedSource%';
SELECT min(value), max(value) FROM test_skip_index_minmax WHERE partition_key = 1;

-- Test: with non part level filter should NOT use skip index
SELECT trimLeft(*) FROM (EXPLAIN SELECT min(value), max(value) FROM test_skip_index_minmax WHERE id < 3) where explain like '%ReadFromMergeTree%';
SELECT min(value), max(value) FROM test_skip_index_minmax WHERE id < 3;

-- Test: with virtual column filter (_partition_id) should use skip index
SELECT trimLeft(*) FROM (EXPLAIN SELECT min(value), max(value) FROM test_skip_index_minmax WHERE _partition_id = '1') where explain like '%ReadFromPreparedSource%';
SELECT min(value), max(value) FROM test_skip_index_minmax WHERE _partition_id = '1';

DROP TABLE test_skip_index_minmax;

-- Test expression-based skip index
DROP TABLE IF EXISTS test_skip_index_expr_minmax;

CREATE TABLE test_skip_index_expr_minmax (
    id UInt64,
    a Int32,
    b Int32,
    partition_key String,
    INDEX skip_minmax_expr (a * b) TYPE minmax GRANULARITY 1
) ENGINE = MergeTree
PARTITION BY partition_key
ORDER BY id;

INSERT INTO test_skip_index_expr_minmax VALUES
    (1, 10, 5, 'p1'),
    (2, 20, 3, 'p1'),
    (3, 5, 2, 'p1'),
    (4, 30, 10, 'p2'),
    (5, 15, 4, 'p2'),
    (6, 2, 3, 'p3'),
    (7, 40, 10, 'p3');

-- Test: with non filter should use skip index
SELECT trimLeft(*) FROM (EXPLAIN SELECT min(a * b), max(a * b) FROM test_skip_index_expr_minmax) where explain like '%ReadFromPreparedSource%';
SELECT min(a * b), max(a * b) FROM test_skip_index_expr_minmax;

-- Test: with partition filter should use skip index
SELECT trimLeft(*) FROM (EXPLAIN SELECT min(a * b), max(a * b) FROM test_skip_index_expr_minmax WHERE partition_key = 'p1') where explain like '%ReadFromPreparedSource%';
SELECT min(a * b), max(a * b) FROM test_skip_index_expr_minmax WHERE partition_key = 'p1';

-- Test: with individual columns filter should NOT use skip index
SELECT trimLeft(*) FROM (EXPLAIN SELECT min(a), max(b) FROM test_skip_index_expr_minmax) where explain like '%ReadFromMergeTree%';
SELECT min(a), max(b) FROM test_skip_index_expr_minmax;

DROP TABLE test_skip_index_expr_minmax;
