DROP TABLE IF EXISTS a;
DROP TABLE IF EXISTS b;
DROP TABLE IF EXISTS c;
DROP TABLE IF EXISTS d;
DROP TABLE IF EXISTS e;

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

DROP TABLE a;
DROP TABLE b;
DROP TABLE c;
DROP TABLE d;
DROP TABLE e;
DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
