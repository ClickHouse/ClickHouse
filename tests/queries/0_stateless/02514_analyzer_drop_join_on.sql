DROP TABLE IF EXISTS a;
DROP TABLE IF EXISTS b;
DROP TABLE IF EXISTS c;
DROP TABLE IF EXISTS d;

CREATE TABLE a (k UInt64, a1 UInt64, a2 String) ENGINE = Memory;
INSERT INTO a VALUES (1, 1, 'a'), (2, 2, 'b'), (3, 3, 'c');

CREATE TABLE b (k UInt64, b1 UInt64, b2 String) ENGINE = Memory;
INSERT INTO b VALUES (1, 1, 'a'), (2, 2, 'b'), (3, 3, 'c');

CREATE TABLE c (k UInt64, c1 UInt64, c2 String) ENGINE = Memory;
INSERT INTO c VALUES (1, 1, 'a'), (2, 2, 'b'), (3, 3, 'c');

CREATE TABLE d (k UInt64, d1 UInt64, d2 String) ENGINE = Memory;
INSERT INTO d VALUES (1, 1, 'a'), (2, 2, 'b'), (3, 3, 'c');

SET enable_analyzer = 1;

-- { echoOn }

EXPLAIN PLAN header = 1
SELECT count() FROM a JOIN b ON b.b1 = a.a1 JOIN c ON c.c1 = b.b1 JOIN d ON d.d1 = c.c1 GROUP BY a.a2
;

EXPLAIN PLAN header = 1
SELECT a.a2, d.d2 FROM a JOIN b USING (k) JOIN c USING (k) JOIN d USING (k)
;

EXPLAIN PLAN header = 1
SELECT b.bx FROM a
JOIN (SELECT b1, b2 || 'x'  AS bx FROM b ) AS b ON b.b1 = a.a1
JOIN c ON c.c1 = b.b1
JOIN (SELECT number AS d1 from numbers(10)) AS d ON d.d1 = c.c1
WHERE c.c2 != '' ORDER BY a.a2
;

-- { echoOff }

DROP TABLE IF EXISTS a;
DROP TABLE IF EXISTS b;
DROP TABLE IF EXISTS c;
DROP TABLE IF EXISTS d;
