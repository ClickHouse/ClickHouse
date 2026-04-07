-- Tests for NATURAL JOIN syntax.
-- NATURAL JOIN automatically joins on all columns with matching names,
-- equivalent to JOIN ... USING (col1, col2, ...) for all common column names.

CREATE TABLE t1 (id UInt64, name String) ENGINE = Memory;
CREATE TABLE t2 (id UInt64, value UInt64) ENGINE = Memory;
INSERT INTO t1 VALUES (1, 'a'), (2, 'b');
INSERT INTO t2 VALUES (1, 100), (3, 300);

-- NATURAL INNER JOIN: equivalent to JOIN t2 USING (id), id appears once
SELECT * FROM t1 NATURAL JOIN t2 ORDER BY id;

-- NATURAL LEFT JOIN
SELECT * FROM t1 NATURAL LEFT JOIN t2 ORDER BY id;

-- NATURAL RIGHT JOIN
SELECT * FROM t1 NATURAL RIGHT JOIN t2 ORDER BY id;

-- NATURAL FULL JOIN
SELECT * FROM t1 NATURAL FULL JOIN t2 ORDER BY id;

-- NATURAL JOIN with multiple common columns
CREATE TABLE t3 (id UInt64, name String, extra UInt64) ENGINE = Memory;
INSERT INTO t3 VALUES (1, 'a', 10), (2, 'b', 20), (2, 'x', 99);
SELECT * FROM t1 NATURAL JOIN t3 ORDER BY id, extra;

-- NATURAL JOIN with subqueries
SELECT * FROM (SELECT id, name FROM t1) AS q1 NATURAL JOIN (SELECT id, value FROM t2) AS q2 ORDER BY id;

-- Old analyzer path
SET allow_experimental_analyzer = 0;
SELECT * FROM t1 NATURAL JOIN t2 ORDER BY id;
SELECT * FROM t1 NATURAL LEFT JOIN t2 ORDER BY id;
SET allow_experimental_analyzer = 1;

SELECT * FROM t1 NATURAL JOIN t2 USING (id); -- { clientError SYNTAX_ERROR } Error: NATURAL JOIN cannot specify USING
SELECT * FROM t1 NATURAL JOIN t2 ON t1.id = t2.id; -- { clientError SYNTAX_ERROR } Error: NATURAL JOIN cannot specify ON
SELECT * FROM t1 NATURAL CROSS JOIN t2; -- { clientError SYNTAX_ERROR } Error: NATURAL JOIN cannot be CROSS JOIN

-- No common columns: NATURAL JOIN degrades to CROSS JOIN
CREATE TABLE t4 (x UInt64) ENGINE = Memory;
INSERT INTO t4 VALUES (1);
SELECT * FROM t1 NATURAL JOIN t4 ORDER BY id;

-- Multi-table joins: NATURAL JOIN mixed with other join types
CREATE TABLE t5 (id UInt64, flag UInt64) ENGINE = Memory;
INSERT INTO t5 VALUES (1, 42);

-- Two chained NATURAL JOINs: second one matches on both id and name
SELECT * FROM t1 NATURAL JOIN t2 NATURAL JOIN t3 ORDER BY id;

-- NATURAL JOIN first, then a regular LEFT JOIN
SELECT * FROM t1 NATURAL JOIN t2 LEFT JOIN t5 USING (id) ORDER BY id;

-- Regular INNER JOIN first, then NATURAL JOIN
SELECT * FROM t1 INNER JOIN t5 USING (id) NATURAL JOIN t2 ORDER BY id;

-- NATURAL FULL JOIN first, then a regular LEFT JOIN
SELECT * FROM t1 NATURAL FULL JOIN t2 LEFT JOIN t5 USING (id) ORDER BY id;

-- CROSS JOIN first, then NATURAL JOIN (cross product feeds into natural join on id)
SELECT * FROM t1 CROSS JOIN t4 NATURAL JOIN t5 ORDER BY id;

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
DROP TABLE t4;
DROP TABLE t5;
