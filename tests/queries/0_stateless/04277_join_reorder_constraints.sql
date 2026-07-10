-- Tags: long
DROP TABLE IF EXISTS a;
DROP TABLE IF EXISTS b;
DROP TABLE IF EXISTS c;
DROP TABLE IF EXISTS d;
DROP TABLE IF EXISTS e;

SET enable_analyzer = 1;
SET query_plan_optimize_join_order_randomize = 1;

CREATE TABLE a (x Int32, z String, w Int32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE b (x Int32, y String, w Int32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE c (y String, z String, w Int32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE d (z Int32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE e (y String, z String) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO a VALUES (1, 'a', 100), (2, 'b', 200);
INSERT INTO b VALUES (1, 'p', 100), (3, 'q', 999);
INSERT INTO b SELECT number + 100, toString(number), number FROM numbers(10);
INSERT INTO c VALUES ('p', 'a', 100), ('q', 'b', 999);
INSERT INTO d VALUES (1), (2);
INSERT INTO e VALUES ('p', 'a'), ('q', 'b');

SELECT '--';

SELECT a.x, a.z, b.x, b.y, c.y, c.z
FROM a LEFT JOIN b ON a.x = b.x
JOIN c ON b.y = c.y AND a.z = c.z
ORDER BY ALL;

SELECT '--';

SELECT count()
FROM a LEFT JOIN b ON a.x = b.x
JOIN c ON b.y = c.y AND a.w = c.w
JOIN d ON a.w = d.z;

SELECT '--';

SELECT count()
FROM a LEFT JOIN b ON a.x = b.x
JOIN c ON a.z = c.z;

SELECT '--';

SELECT count()
FROM (SELECT b.y AS by FROM a LEFT JOIN b ON a.x = b.x) ab
JOIN (SELECT e.y AS ey FROM d LEFT JOIN e ON d.z = toInt32(length(e.z))) de ON ab.by = de.ey;

SELECT '--';

SELECT count()
FROM (SELECT a.x AS id, b.w AS bx FROM a LEFT JOIN b ON a.x = b.x) AS l
INNER JOIN (SELECT d.z AS id, length(e.z) AS dx FROM d LEFT JOIN e ON d.z = toInt32(length(e.z))) AS r
ON l.id = r.id AND l.bx + r.dx > 10
;

SELECT '--';

SELECT count()
FROM a LEFT JOIN b ON a.x = b.x
LEFT JOIN c ON b.y = c.y
JOIN d ON toInt32(length(c.y)) = d.z;

SELECT '--';

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;
CREATE TABLE t1 (c Int32) ENGINE = Memory;
CREATE TABLE t2 (c Int32) ENGINE = Memory;
CREATE TABLE t3 (c Int32) ENGINE = Memory;
INSERT INTO t1 VALUES (1), (2), (3);
INSERT INTO t2 VALUES (1), (10), (20);
INSERT INTO t3 VALUES (99);

SELECT t3.c, t2.c
FROM t2 RIGHT JOIN t3 ON TRUE
INNER JOIN t1 ON t2.c = t1.c
ORDER BY t3.c, t2.c;


SELECT t3.c, t2.c
FROM t2 RIGHT JOIN t3 ON t2.c = t3.c AND t2.c IS NULL
INNER JOIN t1 ON t2.c = t1.c
ORDER BY t3.c, t2.c;

SELECT '--';

-- An outer join comma-joined (cross) with a third table. The cross join must not
-- drop the outer-join restriction of the null-supplying relation: the unmatched
-- rows of the RIGHT/LEFT join must survive the cartesian product.
SELECT t1.c, t2.c, t3.c
FROM t1 RIGHT JOIN t2 ON t1.c = t2.c, t3
ORDER BY t1.c, t2.c, t3.c;

SELECT '--';

SELECT t1.c, t2.c, t3.c
FROM t1 LEFT JOIN t2 ON t1.c = t2.c, t3
ORDER BY t1.c, t2.c, t3.c;

SELECT '--';

-- The null-supplying side of an outer join can be a composite relation (r1 JOIN r2).
-- The predicate `r1.x = r2.x` of the top inner join references only that composite
-- relation: it must be applied as a filter after the outer join, not in its ON clause.
DROP TABLE IF EXISTS r1;
DROP TABLE IF EXISTS r2;
DROP TABLE IF EXISTS r3;
DROP TABLE IF EXISTS r4;
CREATE TABLE r1 (id Int32, x Int32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE r2 (id Int32, x Int32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE r3 (id Int32, y Int32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE r4 (id Int32, z Int32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO r1 VALUES (1, 10), (2, 20), (3, 30), (4, 40), (4, 41), (6, 60);
INSERT INTO r2 VALUES (1, 10), (2, 99), (3, 30), (3, 30), (4, 40), (5, 50);
INSERT INTO r3 VALUES (1, 100), (2, 200), (3, 300), (7, 700), (7, 701);
INSERT INTO r4 VALUES (1, 1000), (2, 2000), (3, 3000), (3, 3001), (7, 7000), (8, 8000);

SELECT * FROM r1 JOIN r2 ON r1.id = r2.id
RIGHT JOIN r3 ON r1.id = r3.id
JOIN r4 ON r1.id = r4.id AND r1.x = r2.x
ORDER BY ALL;

SELECT '--';

SELECT * FROM r1 JOIN r2 ON r1.id = r2.id
RIGHT JOIN r3 ON r1.id = r3.id
JOIN r4 ON r3.id = r4.id
ORDER BY ALL;

DROP TABLE a;
DROP TABLE b;
DROP TABLE c;
DROP TABLE d;
DROP TABLE e;
DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
DROP TABLE r1;
DROP TABLE r2;
DROP TABLE r3;
DROP TABLE r4;
