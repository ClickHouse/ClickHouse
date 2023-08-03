SET enable_optimize_predicate_expression = 0;
SET optimize_move_to_prewhere = 1;
SET convert_query_to_cnf = 0;
SET reorder_joins = 1;

SELECT * FROM system.one l CROSS JOIN system.one r;

DROP TABLE IF EXISTS t1_00826r;
DROP TABLE IF EXISTS t2_00826r;

CREATE TABLE t1_00826r (a Int8, b Nullable(Int8)) ENGINE = Memory;
CREATE TABLE t2_00826r (a Int8, b Nullable(Int8)) ENGINE = Memory;
CREATE TABLE t3_00826r (c Int8, d Nullable(Int8)) ENGINE = Memory;
CREATE TABLE t4_00826r (a Int8, b Int8, c Int8, d Int8) ENGINE = Memory;

INSERT INTO t1_00826r values (1,1), (2,2);
INSERT INTO t2_00826r values (1,1), (1,2);
INSERT INTO t2_00826r (a) values (2), (3);
INSERT INTO t3_00826r values (1,1), (2,2), (3,3), (4,4);
INSERT INTO t4_00826r values (1,2,3,4);

SELECT '--- cross, different name ---';
SELECT a, b, c, d FROM t1_00826r CROSS JOIN t3_00826r WHERE t1_00826r.a = t3_00826r.c ORDER BY t1_00826r.a, t1_00826r.b;
SELECT '--- cross, different name 2 ---';
SELECT sum(a+b+c+d) FROM (SELECT a, b, c, d FROM t3_00826r CROSS JOIN t1_00826r ORDER BY t1_00826r.a ASC NULLS FIRST);
SELECT '--- cross, different name 3 ---';
SELECT c, d FROM t2_00826r INNER JOIN (SELECT c, * FROM t1_00826r CROSS JOIN t3_00826r ORDER BY t1_00826r.a ASC NULLS FIRST) AS ts USING (a) ORDER BY c;
SELECT '--- cross, different name, subquery ---';
SELECT a, b, c, d FROM t2_00826r t2 join (
    SELECT * FROM t1_00826r CROSS JOIN t3_00826r WHERE t1_00826r.a = t3_00826r.c
) ts USING a ORDER BY a, b, c, d;
SELECT '--- cross, different name, subquery, except ---';
SELECT * EXCEPT (b, c) FROM t2_00826r t2 join (
        SELECT * EXCEPT d FROM t1_00826r CROSS JOIN t3_00826r WHERE t1_00826r.a = t3_00826r.c
) ts USING a ORDER BY a, c;
SELECT '--- cross, different name, subquery, replace + except ---';
SELECT * REPLACE (b+1 as b) EXCEPT c FROM t2_00826r t2 join (
        SELECT * EXCEPT d FROM t1_00826r CROSS JOIN t3_00826r WHERE t1_00826r.a = t3_00826r.c
) ts USING a ORDER BY a, b;
SELECT '--- cross, different name, subquery, replace + except inner ---';
SELECT * REPLACE (b+1 as b) EXCEPT c FROM t2_00826r t2 join (
        SELECT * REPLACE (b-1 as b) EXCEPT d FROM t1_00826r CROSS JOIN t3_00826r WHERE t1_00826r.a = t3_00826r.c
) ts USING a ORDER BY a, b;
SELECT '--- cross ---';
SELECT * FROM t1_00826r CROSS JOIN t2_00826r WHERE t1_00826r.a = t2_00826r.a ORDER BY t1_00826r.a, t1_00826r.b, t2_00826r.a, t2_00826r.b;
SELECT '--- cross nullable ---';
SELECT * FROM t1_00826r CROSS JOIN t2_00826r WHERE t1_00826r.b = t2_00826r.b ORDER BY t1_00826r.a, t1_00826r.b;
SELECT '--- cross nullable vs not nullable ---';
SELECT * FROM t1_00826r CROSS JOIN t2_00826r WHERE t1_00826r.a = t2_00826r.b ORDER BY t1_00826r.a, t1_00826r.b;
SELECT '--- cross self ---';
SELECT * FROM t1_00826r x CROSS JOIN t1_00826r y WHERE x.a = y.a AND x.b = y.b ORDER BY x.a;
SELECT '--- cross one table expr ---';
SELECT * FROM t1_00826r CROSS JOIN t2_00826r WHERE t1_00826r.a = t1_00826r.b ORDER BY (t1_00826r.a, t2_00826r.a, t2_00826r.b);
SELECT '--- cross multiple ands ---';
SELECT * FROM t1_00826r CROSS JOIN t2_00826r WHERE t1_00826r.a = t2_00826r.a AND t1_00826r.b = t2_00826r.b;
SELECT '--- cross and inside and ---';
SELECT * FROM t1_00826r CROSS JOIN t2_00826r WHERE t1_00826r.a = t2_00826r.a AND (t1_00826r.b = t2_00826r.b AND 1);
SELECT '--- cross split conjunction ---';
SELECT * FROM t1_00826r CROSS JOIN t2_00826r WHERE t1_00826r.a = t2_00826r.a AND t1_00826r.b = t2_00826r.b AND t1_00826r.a >= 1 AND t2_00826r.b = 1;

SELECT '--- and or ---';
SELECT * FROM t1_00826r CROSS JOIN t2_00826r WHERE t1_00826r.a = t2_00826r.a AND t1_00826r.b = t2_00826r.b AND (t1_00826r.a >= 1 OR t2_00826r.b = 1);

SELECT '--- arithmetic expr ---';
SELECT * FROM t1_00826r CROSS JOIN t2_00826r WHERE t1_00826r.a + 1 = t2_00826r.a + t2_00826r.b AND (t1_00826r.a + t1_00826r.b + t2_00826r.a + t2_00826r.b > 5);

SELECT '--- is null or ---';
SELECT * FROM t1_00826r CROSS JOIN t2_00826r WHERE t1_00826r.b = t2_00826r.a AND (t2_00826r.b IS NULL OR t2_00826r.b > t2_00826r.a) ORDER BY t1_00826r.a;

SELECT '--- do not rewrite alias ---';
SELECT a AS b FROM t1_00826r CROSS JOIN t2_00826r WHERE t1_00826r.b = t2_00826r.a AND b > 0 ORDER BY t1_00826r.a;

SELECT '--- comma ---';
SELECT * FROM t1_00826r, t2_00826r WHERE t1_00826r.a = t2_00826r.a ORDER BY t1_00826r.a, t2_00826r.b;
SELECT '--- comma nullable ---';
SELECT * FROM t1_00826r, t2_00826r WHERE t1_00826r.b = t2_00826r.b ORDER BY t1_00826r.a;
SELECT '--- comma and or ---';
SELECT * FROM t1_00826r, t2_00826r WHERE t1_00826r.a = t2_00826r.a AND (t2_00826r.b IS NULL OR t2_00826r.b < 2) ORDER BY t1_00826r.a;

SELECT '--- column names not affected by reordering ---';
SELECT d0.b FROM t1_00826r d0 join (
                        SELECT d1.b
                        FROM t1_00826r AS d1
                        INNER JOIN t2_00826r AS d2 ON d1.b = d2.b
                        WHERE d1.b > 1
                        ORDER BY d1.b
                    ) s0 USING b;
SELECT d0.b FROM t1_00826r d0 join (
                        SELECT d2.b AS b
                        FROM t1_00826r AS d1
                        INNER JOIN t2_00826r AS d2 ON d1.b = d2.b
                        WHERE d1.b > 1
                        ORDER BY d1.b
                    ) s0 USING b;
SELECT '--- short name for d1 ---';
SELECT d1.a, d1.b, d2.a, d2.b
        FROM t1_00826r AS d1
        INNER JOIN t2_00826r AS d2 ON d1.b = d2.b
        WHERE d1.b > 1
        ORDER BY d1.b limit 0 FORMAT TabSeparatedWithNames;
SELECT '--- short name for d2 ---';
SELECT d1.a, d1.b, d2.a, d2.b
        FROM t2_00826r AS d2
        INNER JOIN t1_00826r AS d1 ON d1.b = d2.b
        WHERE d1.b > 1
        ORDER BY d1.b limit 0 FORMAT TabSeparatedWithNames;

SELECT '--- correct values for columns ---';
SELECT a,b,c,d FROM t4_00826r d0 join (
                        SELECT d2.a AS a
                        FROM t1_00826r AS d1
                        INNER JOIN t2_00826r AS d2 ON d1.b = d2.b
                        WHERE d1.b > 1
                        ORDER BY d1.b
                    ) s0 USING a;

DROP TABLE t1_00826r;
DROP TABLE t2_00826r;
