-- Tags: no-parallel-replicas

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;

CREATE TABLE t1 (a UInt32, x UInt32) ENGINE = MergeTree() ORDER BY a;
CREATE TABLE t2 (b UInt32, y UInt32) ENGINE = MergeTree() ORDER BY b;
CREATE TABLE t3 (c UInt32, z UInt32) ENGINE = MergeTree() ORDER BY c;

INSERT INTO t1 SELECT number, number * 10 FROM numbers(100);
INSERT INTO t2 SELECT number, number * 20 FROM numbers(100);
INSERT INTO t3 SELECT number, number * 30 FROM numbers(100);

SET enable_analyzer = 1;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_join_swap_table = 'auto';
SET query_plan_optimize_join_order_algorithm = 'greedy';
SET query_plan_optimize_prewhere = 1;
SET optimize_move_to_prewhere = 1;
SET enable_join_runtime_filters = 1;

-- Determinism: same seed produces same plan twice
SET query_plan_optimize_join_order_randomize = 42;
EXPLAIN PLAN
SELECT * FROM t1 JOIN t2 ON a = b JOIN t3 ON b = c
FORMAT LineAsString;

SET query_plan_optimize_join_order_randomize = 42;
EXPLAIN PLAN
SELECT * FROM t1 JOIN t2 ON a = b JOIN t3 ON b = c
FORMAT LineAsString;

-- Correctness: results are the same with and without randomization
SET query_plan_optimize_join_order_randomize = 0;
SELECT count(), sum(a), sum(x), sum(y), sum(z)
FROM t1 JOIN t2 ON a = b JOIN t3 ON b = c;

SET query_plan_optimize_join_order_randomize = 42;
SELECT count(), sum(a), sum(x), sum(y), sum(z)
FROM t1 JOIN t2 ON a = b JOIN t3 ON b = c;

SET query_plan_optimize_join_order_randomize = 12345;
SELECT count(), sum(a), sum(x), sum(y), sum(z)
FROM t1 JOIN t2 ON a = b JOIN t3 ON b = c;

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
