-- `optimize_push_subcolumns_into_subqueries` must not use the accesses of a source nested inside the projection
-- of the rewritten source (here a scalar subquery): cloning the outer source replaces the subtree they point into.

SET enable_analyzer = 1;
SET optimize_push_subcolumns_into_subqueries = 1;

DROP TABLE IF EXISTS t1_subcolumn_pushdown_nested;
DROP TABLE IF EXISTS t2_subcolumn_pushdown_nested;
CREATE TABLE t1_subcolumn_pushdown_nested (tup Tuple(a UInt32, b String)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t2_subcolumn_pushdown_nested (tup Tuple(a UInt32, b String)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t1_subcolumn_pushdown_nested VALUES ((1, 'x')), ((2, 'y'));
INSERT INTO t2_subcolumn_pushdown_nested VALUES ((10, 'p')), ((20, 'q'));

SELECT o.r.a, o.m FROM
(
    SELECT tup AS r, (SELECT max(s.r.a) FROM (SELECT tup AS r FROM t2_subcolumn_pushdown_nested) AS s) AS m
    FROM (SELECT tup FROM t1_subcolumn_pushdown_nested)
) AS o
ORDER BY ALL;

SELECT o.r.a, o.r.b, o.m FROM
(
    SELECT tup AS r, (SELECT max(s.r.b) FROM (SELECT tup AS r FROM t2_subcolumn_pushdown_nested) AS s) AS m
    FROM t1_subcolumn_pushdown_nested
) AS o
ORDER BY ALL;

DROP TABLE t1_subcolumn_pushdown_nested;
DROP TABLE t2_subcolumn_pushdown_nested;
