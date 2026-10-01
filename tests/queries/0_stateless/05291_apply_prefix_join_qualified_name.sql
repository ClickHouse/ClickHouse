-- An `APPLY` name prefix goes on the column's projection name, including the qualifier that
-- tells same-named columns of joined tables apart, so wrapping the query in `SELECT * FROM (...)`
-- returns the same result. https://github.com/ClickHouse/ClickHouse/issues/122327

SELECT '-- JOIN USING';
SELECT DISTINCT * APPLY (toTypeName, 'f_') FROM (SELECT 255 AS A, 257 AS B) AS X ALL LEFT JOIN (SELECT 257 AS A, 2 AS B) AS Y USING (B) FORMAT TSVWithNames;
SELECT * FROM (SELECT DISTINCT * APPLY (toTypeName, 'f_') FROM (SELECT 255 AS A, 257 AS B) AS X ALL LEFT JOIN (SELECT 257 AS A, 2 AS B) AS Y USING (B)) FORMAT TSVWithNames;

SELECT '-- JOIN ON';
SELECT * APPLY (toString, 'f_') FROM (SELECT 1 AS a, 2 AS b) AS l INNER JOIN (SELECT 3 AS a, 4 AS b) AS r ON l.a < r.a FORMAT TSVWithNames;
SELECT * FROM (SELECT * APPLY (toString, 'f_') FROM (SELECT 1 AS a, 2 AS b) AS l INNER JOIN (SELECT 3 AS a, 4 AS b) AS r ON l.a < r.a) FORMAT TSVWithNames;

SELECT '-- tables';
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
CREATE TABLE t1 (a UInt32, b UInt32) ENGINE = Memory;
CREATE TABLE t2 (a String, b UInt32) ENGINE = Memory;
INSERT INTO t1 SELECT number, number % 3 FROM numbers(6);
INSERT INTO t2 SELECT toString(number * 10), number % 3 FROM numbers(3);
SELECT * APPLY (toString, 'f_') FROM t1 INNER JOIN t2 USING (b) ORDER BY ALL FORMAT TSVWithNames;
SELECT * FROM (SELECT * APPLY (toString, 'f_') FROM t1 INNER JOIN t2 USING (b)) ORDER BY ALL FORMAT TSVWithNames;
DROP TABLE t1;
DROP TABLE t2;

SELECT '-- three joined tables';
SELECT * APPLY (toString, 'f_') FROM (SELECT 1 AS a) AS t1 CROSS JOIN (SELECT 2 AS a) AS t2 CROSS JOIN (SELECT 3 AS a) AS t3 FORMAT TSVWithNames;
SELECT * FROM (SELECT * APPLY (toString, 'f_') FROM (SELECT 1 AS a) AS t1 CROSS JOIN (SELECT 2 AS a) AS t2 CROSS JOIN (SELECT 3 AS a) AS t3) FORMAT TSVWithNames;

SELECT '-- the same prefix on two qualified matchers, and join_use_nulls';
SELECT X.* APPLY (toTypeName, 'f_'), Y.* APPLY (toTypeName, 'f_') FROM (SELECT 255 AS A, 257 AS B) AS X ALL LEFT JOIN (SELECT 257 AS A, 2 AS B) AS Y USING (B) FORMAT TSVWithNames;
SELECT * APPLY (toTypeName, 'f_') FROM (SELECT 1 AS a) AS l LEFT JOIN (SELECT 2 AS a) AS r ON l.a = r.a SETTINGS join_use_nulls = 1 FORMAT TSVWithNames;

SELECT '-- lambda, chained prefixes, named untuple';
SELECT * APPLY (x -> x, 'f_') FROM (SELECT 1 AS a) AS l CROSS JOIN (SELECT 2 AS a) AS r FORMAT TSVWithNames;
SELECT * APPLY (toString, 'p_') APPLY (upper, 'q_') FROM (SELECT 1 AS a) AS l CROSS JOIN (SELECT 2 AS a) AS r FORMAT TSVWithNames;
SELECT * APPLY (untuple, 'f_') FROM (SELECT (1, 2) AS a) AS l CROSS JOIN (SELECT (3, 4) AS a) AS r FORMAT TSVWithNames;
SELECT * FROM (SELECT * APPLY (untuple, 'f_') FROM (SELECT (1, 2) AS a) AS l CROSS JOIN (SELECT (3, 4) AS a) AS r) FORMAT TSVWithNames;

SELECT '-- COLUMNS and tuple matchers';
SELECT COLUMNS('a') APPLY (toString, 'f_') FROM (SELECT 1 AS a) AS l CROSS JOIN (SELECT 2 AS a) AS r FORMAT TSVWithNames;
SELECT COLUMNS(l.a, r.a) APPLY (toString, 'f_') FROM (SELECT 1 AS a) AS l CROSS JOIN (SELECT 2 AS a) AS r FORMAT TSVWithNames;
SELECT tup.* APPLY (toString, 'f_'), tup2.* APPLY (toString, 'f_') FROM (SELECT CAST((1, 2), 'Tuple(x UInt8, y UInt8)') AS tup, CAST((3, 4), 'Tuple(x UInt8, y UInt8)') AS tup2) FORMAT TSVWithNames;

SELECT '-- a single table keeps the short name, REPLACE keeps the replaced column name';
SELECT * APPLY (toString, 'f_') FROM (SELECT 1 AS a, 2 AS b) FORMAT TSVWithNames;
SELECT * REPLACE (a + 10 AS a) FROM (SELECT 1 AS a) AS l CROSS JOIN (SELECT 2 AS a) AS r FORMAT TSVWithNames;
SELECT * REPLACE (untuple(a) AS a) FROM (SELECT (1, 2) AS a) AS l CROSS JOIN (SELECT (3, 4) AS a) AS r FORMAT TSVWithNames;
