DROP TABLE IF EXISTS t1;
CREATE TABLE t1 (x Int32) ENGINE = MergeTree ORDER BY x;
INSERT INTO t1 VALUES (1), (2), (3);

SET enable_analyzer = 1;

SELECT t2.x FROM t1 JOIN t1 as t2 ON t1.x = t2.x GROUP BY t1.x; -- { serverError NOT_AN_AGGREGATE }
SELECT t2.number FROM numbers(10) as t1 JOIN numbers(10) as t2 ON t1.number = t2.number GROUP BY t1.number; -- { serverError NOT_AN_AGGREGATE }
SELECT t2.a FROM (SELECT x as a FROM t1) as t1 JOIN (SELECT x as a FROM t1) as t2 ON t1.a = t2.a GROUP BY t1.a; -- { serverError NOT_AN_AGGREGATE }
SELECT t2.a FROM (SELECT x as a FROM t1 UNION ALL SELECT x as a FROM t1) as t1 JOIN (SELECT x as a FROM t1 UNION ALL SELECT x as a FROM t1) as t2 ON t1.a = t2.a GROUP BY t1.a; -- { serverError NOT_AN_AGGREGATE }
SELECT t2.number FROM numbers(10) JOIN numbers(10) as t2 ON number = t2.number GROUP BY number SETTINGS joined_subquery_requires_alias = 0; -- { serverError NOT_AN_AGGREGATE }

SELECT t2.x FROM t1 as t0 JOIN t1 as t2 ON t1.x = t2.x GROUP BY t1.x; -- { serverError NOT_AN_AGGREGATE }
SELECT t2.x FROM t1 as t0 JOIN t1 as t2 ON t0.x = t2.x GROUP BY t0.x; -- { serverError NOT_AN_AGGREGATE }
SELECT t2.x FROM t1 JOIN t1 as t2 ON t1.x = t2.x GROUP BY x; -- { serverError NOT_AN_AGGREGATE }
SELECT t1.x FROM t1 JOIN t1 as t2 ON t1.x = t2.x GROUP BY t2.x; -- { serverError NOT_AN_AGGREGATE }
SELECT x FROM t1 JOIN t1 as t2 ON t1.x = t2.x GROUP BY t2.x; -- { serverError NOT_AN_AGGREGATE }
SELECT x FROM t1 JOIN t1 as t2 USING (x) GROUP BY t2.x; -- { serverError NOT_AN_AGGREGATE }

SELECT t1.x FROM t1 JOIN t1 as t2 ON t1.x = t2.x GROUP BY x ORDER BY ALL;
SELECT x, sum(t2.x) FROM t1 JOIN t1 as t2 ON t1.x = t2.x GROUP BY t1.x ORDER BY ALL;
