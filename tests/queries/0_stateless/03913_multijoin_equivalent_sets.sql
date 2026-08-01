DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;
DROP TABLE IF EXISTS t4;
DROP TABLE IF EXISTS t5;

CREATE TABLE t1 ( id UInt32, `value` String ) ENGINE = MergeTree() ORDER BY id;
CREATE TABLE t2 ( id UInt32, `value` String ) ENGINE = MergeTree() ORDER BY id;
CREATE TABLE t3 ( id UInt32, `value` String ) ENGINE = MergeTree() ORDER BY id;
CREATE TABLE t4 ( id UInt32, `value` String ) ENGINE = MergeTree() ORDER BY id;
CREATE TABLE t5 ( id UInt32, `value` String ) ENGINE = MergeTree() ORDER BY id;

INSERT INTO t1 SELECT number, toString(number) AS value FROM numbers(100_000);
INSERT INTO t2 SELECT number, toString(number) AS value FROM numbers(100_000);
INSERT INTO t3 SELECT number, toString(number) AS value FROM numbers(100_000);
INSERT INTO t4 SELECT number, toString(number) AS value FROM numbers(100_000);
INSERT INTO t5 SELECT number, toString(number) AS value FROM numbers(100_000);

SET enable_analyzer = 1;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_limit = 0;
SET optimize_move_to_prewhere = 1;
SET enable_join_runtime_filters = 0;
SET enable_parallel_replicas = 0;

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1, keep_logical_steps = 1
    SELECT * FROM t1
    JOIN t2 ON t1.id = t2.id
    JOIN t3 ON t2.id = t3.id
    JOIN t4 ON t3.id = t4.id
    JOIN t5 ON t4.id = t5.id
    WHERE t1.id < 10_000
) WHERE trim(explain) like '%Prewhere filter column%'
;

SELECT '--';

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1, keep_logical_steps = 1
    SELECT * FROM t1
    JOIN t2 ON t1.id = t2.id
    JOIN t3 ON t2.id = t3.id
    JOIN t4 ON t3.id = t4.id
    JOIN t5 ON t4.id = t5.id
    WHERE t5.id < 10_000
) WHERE trim(explain) like '%Prewhere filter column%'
;

SELECT '--';

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1, keep_logical_steps = 1
    SELECT * FROM t1
    LEFT JOIN t2 ON t1.id = t2.id
    JOIN t3 ON t2.id = t3.id
    WHERE t1.id < 10_000
) WHERE trim(explain) like '%Prewhere filter column%'
;
