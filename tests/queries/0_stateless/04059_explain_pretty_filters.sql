SET enable_analyzer = 1;
SET parallel_hash_join_threshold = 0;
SET enable_join_runtime_filters = 0;
SET query_plan_join_swap_table = 0;
SET enable_parallel_replicas = 0;
SET use_statistics = 0;
SET query_plan_optimize_join_order_limit = 0;
SET optimize_move_to_prewhere = 1;
SET query_plan_optimize_prewhere = 1;
SET query_plan_join_shard_by_pk_ranges = 0;
SET allow_reorder_prewhere_conditions = 0;
SET max_bytes_before_external_join = 0;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1 (a UInt64, b String, c Float64) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
CREATE TABLE t2 (x UInt64, y String) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO t1 SELECT number, toString(number), number * 1.5 FROM numbers(1000);
INSERT INTO t2 SELECT number, toString(number) FROM numbers(100);

SELECT '--- Simple filter ---';

EXPLAIN PLAN actions = 1, compact = 1, pretty = 1
SELECT * FROM t1 WHERE a > 5;

SELECT '--- Complex filter AND ---';

EXPLAIN PLAN actions = 1, compact = 1, pretty = 1
SELECT * FROM t1 WHERE a > 1 AND (b != 'x' OR b LIKE '%foo%') AND c + a * 2 < 100;

SELECT '--- Filter with nested expression ---';

EXPLAIN PLAN actions = 1, compact = 1, pretty = 1
SELECT * FROM t1 WHERE ((a + 1) * 2 > 10 OR c / (a + 1) < 5) AND (a % 3 = 0 OR b != 'test');

SELECT '--- Prewhere ---';

EXPLAIN PLAN actions = 1, compact = 1, pretty = 1
SELECT * FROM t1 PREWHERE a > 5 WHERE b = 'x';

SELECT '--- Prewhere with expression ---';

EXPLAIN PLAN actions = 1, compact = 1, pretty = 1
SELECT * FROM t1 PREWHERE a + 1 > 5;

SELECT '--- Runtime filter ---';

EXPLAIN PLAN actions = 1, compact = 1, pretty = 1
SELECT *
FROM t1
INNER JOIN t2 ON t1.a = t2.x
SETTINGS enable_join_runtime_filters = 1, join_algorithm = 'parallel_hash';

DROP TABLE t1;
DROP TABLE t2;
