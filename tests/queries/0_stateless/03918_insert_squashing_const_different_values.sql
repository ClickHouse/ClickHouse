-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/95537
-- INSERT SELECT with UNION ALL + CTE + JOIN corrupts constant values
DROP TABLE IF EXISTS t_const_squash;
CREATE TABLE t_const_squash (s String, x UInt32) ENGINE = MergeTree ORDER BY x;

INSERT INTO t_const_squash(s, x)
WITH a AS (SELECT 'a' AS s, number AS x FROM numbers(100)),
     b AS (SELECT 'b' AS s, number AS x FROM numbers(90))
SELECT s, x FROM a LEFT JOIN numbers(30) num1 ON a.x = num1.number
UNION ALL
SELECT s, x FROM b LEFT JOIN numbers(10) num2 ON b.x = num2.number;

SELECT s, count() FROM t_const_squash GROUP BY s ORDER BY s;

TRUNCATE TABLE t_const_squash;

INSERT INTO t_const_squash(s, x)
WITH a AS (SELECT 'a' AS s, number AS x FROM numbers(100)),
     b AS (SELECT 'b' AS s, number AS x FROM numbers(90)),
     c AS (SELECT 'c' AS s, number AS x FROM numbers(80)),
     d AS (SELECT 'd' AS s, number AS x FROM numbers(70))
SELECT s, x FROM a JOIN numbers(30) num1 ON a.x = num1.number
UNION ALL
SELECT s, x FROM b JOIN numbers(10) num2 ON b.x = num2.number
UNION ALL
SELECT s, x FROM c JOIN numbers(20) num3 ON c.x = num3.number
UNION ALL
SELECT s, x FROM d JOIN numbers(15) num4 ON d.x = num4.number;

SELECT s, count() FROM t_const_squash GROUP BY s ORDER BY s;

DROP TABLE t_const_squash;
