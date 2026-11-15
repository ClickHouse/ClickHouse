SET enable_optimize_predicate_expression = 0;
SET optimize_move_to_prewhere = 1;
SET convert_query_to_cnf = 0;

select * from system.one l cross join system.one r order by all;

DROP TABLE IF EXISTS t1_00826;
DROP TABLE IF EXISTS t2_00826;

CREATE TABLE t1_00826 (a Int8, b Nullable(Int8)) ENGINE = Memory;
CREATE TABLE t2_00826 (a Int8, b Nullable(Int8)) ENGINE = Memory;

INSERT INTO t1_00826 values (1,1), (2,2);
INSERT INTO t2_00826 values (1,1), (1,2);
INSERT INTO t2_00826 (a) values (2), (3);

SELECT '--- cross ---';
SELECT * FROM t1_00826 cross join t2_00826 where t1_00826.a = t2_00826.a ORDER BY ALL;
SELECT '--- cross nullable ---';
SELECT * FROM t1_00826 cross join t2_00826 where t1_00826.b = t2_00826.b ORDER BY ALL;
SELECT '--- cross nullable vs not nullable ---';
SELECT * FROM t1_00826 cross join t2_00826 where t1_00826.a = t2_00826.b ORDER BY t1_00826.a;
SELECT '--- cross self ---';
SELECT * FROM t1_00826 x cross join t1_00826 y where x.a = y.a and x.b = y.b ORDER BY x.a;
SELECT '--- cross one table expr ---';
SELECT * FROM t1_00826 cross join t2_00826 where t1_00826.a = t1_00826.b order by (t1_00826.a, t2_00826.a, t2_00826.b);
SELECT '--- cross multiple ands ---';
select * from t1_00826 cross join t2_00826 where t1_00826.a = t2_00826.a and t1_00826.b = t2_00826.b order by all;
SELECT '--- cross and inside and ---';
select * from t1_00826 cross join t2_00826 where t1_00826.a = t2_00826.a and (t1_00826.b = t2_00826.b and 1) order by all;
SELECT '--- cross split conjunction ---';
select * from t1_00826 cross join t2_00826 where t1_00826.a = t2_00826.a and t1_00826.b = t2_00826.b and t1_00826.a >= 1 and t2_00826.b = 1 order by all;

SELECT '--- and or ---';
select * from t1_00826 cross join t2_00826 where t1_00826.a = t2_00826.a and t1_00826.b = t2_00826.b and (t1_00826.a >= 1 OR t2_00826.b = 1) order by all;

SELECT '--- arithmetic expr ---';
select * from t1_00826 cross join t2_00826 where t1_00826.a + 1 = t2_00826.a + t2_00826.b AND (t1_00826.a + t1_00826.b + t2_00826.a + t2_00826.b > 5) order by all;

SELECT '--- is null or ---';
select * from t1_00826 cross join t2_00826 where t1_00826.b = t2_00826.a AND (t2_00826.b IS NULL OR t2_00826.b > t2_00826.a) ORDER BY t1_00826.a;

SELECT '--- do not rewrite alias ---';
SELECT a as b FROM t1_00826 cross join t2_00826 where t1_00826.b = t2_00826.a AND b > 0 ORDER BY ALL;

SELECT '--- comma ---';
SELECT * FROM t1_00826, t2_00826 where t1_00826.a = t2_00826.a ORDER BY ALL;
SELECT '--- comma nullable ---';
SELECT * FROM t1_00826, t2_00826 where t1_00826.b = t2_00826.b ORDER BY ALL;
SELECT '--- comma and or ---';
SELECT * FROM t1_00826, t2_00826 where t1_00826.a = t2_00826.a AND (t2_00826.b IS NULL OR t2_00826.b < 2)
ORDER BY ALL;


SELECT '--- cross ---';
EXPLAIN SYNTAX select * from t1_00826 cross join t2_00826 where t1_00826.a = t2_00826.a order by all;
SELECT '--- cross nullable ---';
EXPLAIN SYNTAX select * from t1_00826, t2_00826 where t1_00826.a = t2_00826.a order by all;
SELECT '--- cross nullable vs not nullable ---';
EXPLAIN SYNTAX select * from t1_00826 cross join t2_00826 where t1_00826.a = t2_00826.b order by all;
SELECT '--- cross self ---';
EXPLAIN SYNTAX select * from t1_00826 x cross join t1_00826 y where x.a = y.a and x.b = y.b order by all;
SELECT '--- cross one table expr ---';
EXPLAIN SYNTAX select * from t1_00826 cross join t2_00826 where t1_00826.a = t1_00826.b order by all;
SELECT '--- cross multiple ands ---';
EXPLAIN SYNTAX select * from t1_00826 cross join t2_00826 where t1_00826.a = t2_00826.a and t1_00826.b = t2_00826.b order by all;
SELECT '--- cross and inside and ---';
EXPLAIN SYNTAX select * from t1_00826 cross join t2_00826 where t1_00826.a = t2_00826.a and (t1_00826.a = t2_00826.a and (t1_00826.a = t2_00826.a and t1_00826.b = t2_00826.b)) order by all;

SELECT '--- cross split conjunction ---';
EXPLAIN SYNTAX select * from t1_00826 cross join t2_00826 where t1_00826.a = t2_00826.a and t1_00826.b = t2_00826.b and t1_00826.a >= 1 and t2_00826.b > 0 order by all;

SELECT '--- and or ---';
EXPLAIN SYNTAX select * from t1_00826 cross join t2_00826 where t1_00826.a = t2_00826.a and t1_00826.b = t2_00826.b and (t1_00826.a >= 1 OR t2_00826.b = 1) order by all;

SELECT '--- arithmetic expr ---';
EXPLAIN SYNTAX select * from t1_00826 cross join t2_00826 where t1_00826.a + 1 = t2_00826.a + t2_00826.b AND (t1_00826.a + t1_00826.b + t2_00826.a + t2_00826.b > 5) order by all;

SELECT '--- is null or ---';
EXPLAIN SYNTAX select * from t1_00826 cross join t2_00826 where t1_00826.b = t2_00826.a AND (t2_00826.b IS NULL OR t2_00826.b > t2_00826.a) order by all;

SELECT '--- do not rewrite alias ---';
EXPLAIN SYNTAX select a as b from t1_00826 cross join t2_00826 where t1_00826.b = t2_00826.a AND b > 0 order by all;

SELECT '--- comma ---';
EXPLAIN SYNTAX select * from t1_00826, t2_00826 where t1_00826.a = t2_00826.a order by all;
SELECT '--- comma nullable ---';
EXPLAIN SYNTAX select * from t1_00826, t2_00826 where t1_00826.b = t2_00826.b order by all;
SELECT '--- comma and or ---';
EXPLAIN SYNTAX select * from t1_00826, t2_00826 where t1_00826.a = t2_00826.a AND (t2_00826.b IS NULL OR t2_00826.b < 2) order by all;

DROP TABLE t1_00826;
DROP TABLE t2_00826;
