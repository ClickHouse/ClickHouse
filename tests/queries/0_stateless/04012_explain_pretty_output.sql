SET enable_analyzer = 1;
SET parallel_hash_join_threshold = 0;
SET enable_join_runtime_filters = 0;
SET query_plan_join_swap_table = 0;
SET enable_parallel_replicas = 0;
SET use_statistics = 0;
SET query_plan_join_shard_by_pk_ranges = 0;
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0; -- Disable automatic spilling for this test
SET query_plan_optimize_join_order_limit = 10; -- needed for row count estimates and table[N] labels in EXPLAIN output

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1 (a UInt64, b String, c Float64) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
CREATE TABLE t2 (x UInt64, y String) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO t1 SELECT number, toString(number), number * 1.5 FROM numbers(100);
INSERT INTO t2 SELECT number, toString(number) FROM numbers(100);

EXPLAIN PLAN actions = 1, compact = 1, pretty = 1
SELECT a, b FROM t1;

SELECT '--- Projection with expressions ---';

EXPLAIN PLAN actions = 1, compact = 1, pretty = 1
SELECT a + 1 AS a_plus, lower(b) AS b_lower FROM t1;

SELECT '--- ReadFromMergeTree output ---';

EXPLAIN PLAN actions = 1, compact = 1, pretty = 1
SELECT a, b, c FROM t1;

SELECT '--- ReadFromMergeTree with subset of columns ---';

EXPLAIN PLAN actions = 1, compact = 1, pretty = 1
SELECT a FROM t1;

SELECT '--- Join output with left and right columns ---';

EXPLAIN PLAN actions = 1, compact = 1, pretty = 1
SELECT t1.a, t1.b, t2.y
FROM t1
INNER JOIN t2 ON t1.a = t2.x;

SELECT '--- Join output select star ---';

EXPLAIN PLAN actions = 1, compact = 1, pretty = 1
SELECT *
FROM t1
INNER JOIN t2 ON t1.a = t2.x;

SELECT '--- Join with only left columns selected ---';

EXPLAIN PLAN actions = 1, compact = 1, pretty = 1
SELECT t1.a, t1.b
FROM t1
INNER JOIN t2 ON t1.a = t2.x;

SELECT '--- Join with only right columns selected ---';

EXPLAIN PLAN actions = 1, compact = 1, pretty = 1
SELECT t2.x, t2.y
FROM t1
INNER JOIN t2 ON t1.a = t2.x;

SELECT '--- No output lines without pretty ---';

EXPLAIN PLAN actions = 1, compact = 1, pretty = 0
SELECT a, b FROM t1;

DROP TABLE t1;
DROP TABLE t2;
