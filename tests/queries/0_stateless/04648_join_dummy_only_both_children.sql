-- `query_plan_optimize_join_order_limit` is randomized in CI, and only 0 and 1 keep both children
-- of the enclosing join dummy-only, so it is pinned per statement.

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;
DROP TABLE IF EXISTS t4;
DROP TABLE IF EXISTS t5;

CREATE TABLE t1 (a UInt64, b UInt64) ENGINE = Log;
INSERT INTO t1 VALUES (1, 2), (3, 4), (5, 6);

CREATE TABLE t2 (a UInt64, b UInt64) ENGINE = Log;
INSERT INTO t2 VALUES (3, 4), (5, 6), (7, 8);

CREATE TABLE t3 (a UInt64, b UInt64) ENGINE = Log;
INSERT INTO t3 VALUES (5, 6), (7, 8), (9, 10);

CREATE TABLE t4 (a UInt64, b UInt64) ENGINE = Log;
INSERT INTO t4 VALUES (7, 8), (9, 10), (11, 12);

CREATE TABLE t5 (a UInt64, b UInt64) ENGINE = Log;
INSERT INTO t5 VALUES (9, 10), (11, 12), (13, 14);

SELECT 'both children dummy-only';

-- Aborted before the fix. Expected values match the default reordered plan below, so these assert
-- correctness and not just the absence of an abort.
SELECT count() FROM t1, t2, t3, t4 WHERE (t1.b = t2.b) AND (t3.a = t4.a)
SETTINGS query_plan_optimize_join_order_limit = 0;

SELECT count() FROM t1, t2, t3, t4 WHERE (t1.b = t2.b) AND (t3.a = t4.a)
SETTINGS query_plan_optimize_join_order_limit = 1;

SELECT count() FROM t1, t2, t3, t4, t5 WHERE (t1.b = t2.b) AND (t3.a = t4.a)
SETTINGS query_plan_optimize_join_order_limit = 0;

-- Any projection that needs no column from the join reaches the same shape.
SELECT 1 FROM t1, t2, t3, t4 WHERE (t1.b = t2.b) AND (t3.a = t4.a)
SETTINGS query_plan_optimize_join_order_limit = 0;

SELECT toTypeName(t1.a) FROM t1, t2, t3, t4 WHERE (t1.b = t2.b) AND (t3.a = t4.a)
SETTINGS query_plan_optimize_join_order_limit = 0;

SELECT count() FROM t1, t2, t3, t4 WHERE (t1.b = t2.b) AND (t3.a = t4.a)
SETTINGS query_plan_optimize_join_order_limit = 0, any_join_distinct_right_table_keys = 1;

SELECT 'reference values from the reordered plan';

SELECT count() FROM t1, t2, t3, t4 WHERE (t1.b = t2.b) AND (t3.a = t4.a)
SETTINGS query_plan_optimize_join_order_limit = 5;

SELECT count() FROM t1, t2, t3, t4 WHERE (t1.b = t2.b) AND (t3.a = t4.a);

SELECT count() FROM t1, t2, t3, t4, t5 WHERE (t1.b = t2.b) AND (t3.a = t4.a);

SELECT 'shapes that keep real output columns';

-- Negative controls: the enclosing join's two child headers share no column name, so each name's
-- queue holds one input node and resolves identically with and without the fix.
SELECT count() FROM t1 LEFT JOIN t2 ON t1.b = t2.b, t3 LEFT JOIN t4 ON t3.a = t4.a
SETTINGS query_plan_optimize_join_order_limit = 0;

SELECT count() FROM t1 JOIN t2 ON t1.b = t2.b CROSS JOIN t3 JOIN t4 ON t3.a = t4.a
SETTINGS query_plan_optimize_join_order_limit = 0;

-- A derived table projects a real column, so neither child is dummy-only.
SELECT count() FROM (SELECT count() AS c FROM t1, t2 WHERE t1.b = t2.b) AS x,
                    (SELECT count() AS c FROM t3, t4 WHERE t3.a = t4.a) AS y
SETTINGS query_plan_optimize_join_order_limit = 0;

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
DROP TABLE t4;
DROP TABLE t5;
