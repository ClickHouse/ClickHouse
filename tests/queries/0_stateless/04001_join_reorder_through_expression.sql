DROP TABLE IF EXISTS t0;
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;

SET enable_parallel_replicas = 0;
SET query_plan_join_swap_table = 'auto';
SET query_plan_optimize_join_order_limit = 64;

CREATE TABLE t0 (id UInt64, val String) ENGINE = MergeTree ORDER BY id;
INSERT INTO t0 VALUES (0, 'aa'), (1, 'bb'), (2, 'cc');
CREATE TABLE t1 (id UInt64, val String) ENGINE = MergeTree ORDER BY id;
INSERT INTO t1 VALUES (1, 'ODD'), (2, 'EVEN'), (3, 'ODD');

CREATE TABLE t2 (id UInt64, attr UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t2 VALUES (1, 10), (2, 20), (3, 30);

CREATE TABLE t3 (id UInt64, val String) ENGINE = MergeTree ORDER BY id;
INSERT INTO t3 VALUES (11, 'A'), (21, 'B'), (31, 'C');

SET enable_join_runtime_filters = 0;

EXPLAIN PLAN keep_logical_steps = 1, description = 0
SELECT * FROM (
    SELECT t0.id, upper(t0.val) as key, lower(t1.val) as attr
    FROM (SELECT id + 1 as id, val FROM t0) as t0
    JOIN ( SELECT id, val FROM t1 ) as t1
    ON t0.id = t1.id
) as l
INNER JOIN (
    SELECT t2.id, repeat(t3.val, 2) as key
    FROM t2
    JOIN ( SELECT (id - 1) :: UInt64 as id , val FROM t3 ) as t3
    ON t2.attr = t3.id
) as r
ON l.key = r.key
ORDER BY l.id
;

SELECT * FROM (
    SELECT t0.id, upper(t0.val) as key, lower(t1.val) as attr
    FROM (SELECT id + 1 as id, val FROM t0) as t0
    JOIN ( SELECT id, val FROM t1 ) as t1
    ON t0.id = t1.id
) as l
INNER JOIN (
    SELECT t2.id, repeat(t3.val, 2) as key
    FROM t2
    JOIN ( SELECT (id - 1) :: UInt64 as id , val FROM t3 ) as t3
    ON t2.attr = t3.id
) as r
ON l.key = r.key
ORDER BY l.id
;
