SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_dot_col_leak;
CREATE TABLE t_dot_col_leak (id Int) ENGINE = Memory;
INSERT INTO t_dot_col_leak VALUES (1);

WITH
    a AS (SELECT id FROM t_dot_col_leak),
    b AS (SELECT id FROM t_dot_col_leak),
    c AS (SELECT * FROM a INNER JOIN b ON a.id = b.id)
SELECT *
FROM a
LEFT JOIN c ON c.id = a.id
LEFT JOIN b ON b.id = a.id;

-- Single-part quoted identifier still finds a column whose name contains a dot.
SELECT `b.id` FROM (SELECT 1 AS `b.id`);

-- Qualified access via the subquery's own alias still works.
SELECT t.`b.id` FROM (SELECT 1 AS `b.id`) AS t;

-- Nested column access on real storage is unaffected.
DROP TABLE IF EXISTS t_nested_dot_col_leak;
CREATE TABLE t_nested_dot_col_leak (n Nested(x Int)) ENGINE = Memory;
INSERT INTO t_nested_dot_col_leak VALUES ([1]);
SELECT n.x FROM t_nested_dot_col_leak;

-- Nested subcolumn passed through a subquery is still reachable from the outer
-- scope: the guard only fires when the identifier's first part also names a
-- sibling table expression in the join tree.
SELECT n.x FROM (SELECT n.x FROM t_nested_dot_col_leak);

DROP TABLE t_dot_col_leak;
DROP TABLE t_nested_dot_col_leak;
