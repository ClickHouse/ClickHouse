-- https://github.com/ClickHouse/ClickHouse/issues/72459

CREATE TABLE t1 (c1 UInt64, c2 UInt64) ORDER BY c1;
CREATE TABLE t2 (c1 UInt64) ORDER BY c1;

INSERT INTO t1 SELECT number, number % 100 FROM numbers(100);
INSERT INTO t2 SELECT number*number FROM numbers(100);

set enable_analyzer = 1;
set allow_experimental_correlated_subqueries = 1;

-- { echoOn }

SELECT count(t1.c1) FROM t1
WHERE
t1.c2 = 10
AND EXISTS (SELECT * FROM t2 WHERE t1.c1 = t2.c1);

SELECT count(t1.c1) FROM t1
WHERE
t1.c2 = 10
AND t1.c1 IN (SELECT c1 FROM t2);

SELECT count(t1.c1) FROM t1
WHERE
t1.c2 = 10
AND NOT EXISTS (SELECT * FROM t2 WHERE t1.c1 = t2.c1);

SELECT count(t1.c1) FROM t1
WHERE
t1.c2 = 10
AND t1.c1 NOT IN (SELECT c1 FROM t2);
