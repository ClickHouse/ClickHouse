SET enable_analyzer = 1;
SET parallel_hash_join_threshold = 0;
SET enable_join_runtime_filters = 0;
SET query_plan_join_swap_table = 0;
SET enable_parallel_replicas = 0;
SET use_statistics = 0;
SET query_plan_optimize_join_order_limit = 0;
SET query_plan_join_shard_by_pk_ranges = 0;
SET optimize_move_to_prewhere = 1;
SET query_plan_optimize_prewhere = 1;
SET allow_reorder_prewhere_conditions = 0;
SET max_bytes_before_external_join = 0;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;
DROP TABLE IF EXISTS s;

CREATE TABLE t1 (a UInt64, b String) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
CREATE TABLE t2 (x UInt64, y String) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
CREATE TABLE t3 (id UInt64, val String) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO t1 SELECT number, toString(number) FROM numbers(100);
INSERT INTO t2 SELECT number, toString(number) FROM numbers(100);
INSERT INTO t3 SELECT number, toString(number) FROM numbers(100);

SELECT '--- Join single condition ---';

EXPLAIN PLAN actions = 1, compact = 1, pretty = 1
SELECT * FROM t1 INNER JOIN t2 ON t1.a = t2.x;

SELECT '--- Join null-safe ---';

EXPLAIN PLAN actions = 1, compact = 1, pretty = 1
SELECT * FROM t1 JOIN t2 ON t1.a <=> t2.x;

SELECT '--- IN small tuple set ---';

EXPLAIN PLAN actions = 1, compact = 1, pretty = 1
SELECT * FROM t1 WHERE a IN (1, 2, 3);

SELECT '--- IN large tuple set with truncation ---';

EXPLAIN PLAN actions = 1, compact = 1, pretty = 1
SELECT * FROM t1 WHERE a IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12);

SELECT '--- NOT IN ---';

EXPLAIN PLAN actions = 1, compact = 1, pretty = 1
SELECT * FROM t1 WHERE a NOT IN (1, 2, 3);

SELECT '--- Multi-key IN ---';

EXPLAIN PLAN actions = 1, compact = 1, pretty = 1
SELECT * FROM t1 WHERE (a, b) IN ((1, 'x'), (2, 'y'));

SELECT '--- IN subquery ---';

EXPLAIN PLAN actions = 1, compact = 1, pretty = 1
SELECT * FROM t1 WHERE a IN (SELECT x FROM t2);

SELECT '--- Multiple subquery sets ---';

EXPLAIN PLAN actions = 1, compact = 1, pretty = 1
SELECT * FROM t1 WHERE a IN (SELECT x FROM t2) AND b IN (SELECT y FROM t2);

SELECT '--- IN with Set engine ---';

CREATE TABLE s (a UInt64) ENGINE = Set;
INSERT INTO s VALUES (1), (2), (3);

EXPLAIN PLAN actions = 1, compact = 1, pretty = 1
SELECT * FROM t1 WHERE a IN s;

DROP TABLE s;
DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
