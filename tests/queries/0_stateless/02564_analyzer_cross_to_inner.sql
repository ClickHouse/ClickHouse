SET enable_analyzer = 1;

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

SET cross_to_inner_join_rewrite = 1;

SELECT * FROM t1, t2, (SELECT a as x from t3 where a + 1 = b ) as t3
WHERE t1.a = if(t2.b > 0, t2.a, 0) AND t2.a = t3.x AND 1
;

SELECT * FROM t1, t2, (SELECT a as x from t3 where a + 1 = b ) as t3
WHERE t1.a = if(t2.b > 0, t2.a, 0)
ORDER BY t1.a, t2.a, t3.x
;

SELECT * FROM t1, t2, t3, t4, t5
WHERE t2.a = t3.a AND t1.b = t5.b;

SELECT * FROM t1, t2, t3, t4, t5
WHERE t2.a = t3.a AND t1.b = t5.b AND t4.a = t5.a;

SELECT * FROM t1, t2, t3, t4, t5
WHERE t1.a = t3.a AND t3.b = t4.b AND t1.a = t4.a AND t2.a = t5.a;

SELECT * FROM t1, t2, t3, t4, t5
WHERE t1.a = t2.a AND t1.a = t3.a AND t1.a = t4.a AND t1.a = t5.a
AND t2.a = t3.a AND t2.a = t4.a AND t2.a = t5.a
AND t3.a = t4.a AND t3.a = t5.a
AND t4.a = t5.a;

-- { echoOn }

EXPLAIN QUERY TREE
SELECT * FROM t1, t2, (SELECT a as x from t3 where a + 1 = b ) as t3
WHERE t1.a = if(t2.b > 0, t2.a, 0) AND t2.a = t3.x AND 1;

EXPLAIN QUERY TREE
SELECT * FROM t1, t2, (SELECT a as x from t3 where a + 1 = b ) as t3
WHERE t1.a = if(t2.b > 0, t2.a, 0) AND t2.a = t3.x AND 1
SETTINGS cross_to_inner_join_rewrite = 0;

EXPLAIN QUERY TREE
SELECT * FROM t1, t2, (SELECT a as x from t3 where a + 1 = b ) as t3
WHERE t1.a = if(t2.b > 0, t2.a, 0);

EXPLAIN QUERY TREE dump_ast=1
SELECT * FROM t1, t2, t3, t4, t5
WHERE t2.a = t3.a AND t1.b = t5.b;

EXPLAIN QUERY TREE dump_ast=1
SELECT * FROM t1, t2, t3, t4, t5
WHERE t2.a = t3.a AND t1.b = t5.b AND t4.a = t5.a;

EXPLAIN QUERY TREE dump_ast=1
SELECT * FROM t1, t2, t3, t4, t5
WHERE t1.a = t3.a AND t3.b = t4.b AND t1.a = t4.a AND t2.a = t5.a;

EXPLAIN QUERY TREE dump_ast=1
SELECT * FROM t1, t2, t3, t4, t5
WHERE t1.a = t2.a AND t1.a = t3.a AND t1.a = t4.a AND t1.a = t5.a
AND t2.a = t3.a AND t2.a = t4.a AND t2.a = t5.a
AND t3.a = t4.a AND t3.a = t5.a
AND t4.a = t5.a;

-- { echoOff }

SELECT * FROM t1, t2, (SELECT a as x from t3 where a + 1 = b ) as t3
WHERE t1.a = if(t2.b > 0, t2.a, 0)
SETTINGS cross_to_inner_join_rewrite = 2; -- { serverError INCORRECT_QUERY }

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;
