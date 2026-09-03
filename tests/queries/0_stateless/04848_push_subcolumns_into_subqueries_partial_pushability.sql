-- Tags: no-parallel-replicas, no-random-settings
-- The test checks EXPLAIN output, which differs with parallel replicas and randomized plan-related settings.

SET enable_analyzer = 1;
SET optimize_push_subcolumns_into_subqueries = 1;

DROP TABLE IF EXISTS t_push_subcolumns_partial;

-- The ordinary column `tup.a` shadows the same-named subcolumn of `tup` inside the subquery,
-- so the `a` subcolumn of the exported `tup` cannot be pushed down, while the `b` subcolumn can.
-- The Memory engine is used because MergeTree rejects such a column (data stream name collision).
CREATE TABLE t_push_subcolumns_partial (id UInt32, tup Tuple(a UInt32, b String), `tup.a` UInt32)
ENGINE = Memory;

INSERT INTO t_push_subcolumns_partial VALUES (1, (1, 'one'), 10), (2, (2, 'two'), 20);

-- Two subcolumns of the same exported column are requested, and only one of them is pushable.
-- Pushing the pushable one alone would make the subquery read both the whole column and the
-- subcolumn (the unpushable sibling keeps the whole column alive), so nothing is pushed.

SELECT 'two subcolumns, only one pushable: nothing is pushed';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT s.tup.a, s.tup.b FROM (SELECT tup FROM t_push_subcolumns_partial) AS s) WHERE explain LIKE '%Output%';
SELECT s.tup.a, s.tup.b FROM (SELECT tup FROM t_push_subcolumns_partial) AS s ORDER BY 1;

SELECT 'the unpushable subcolumn alone is not pushed';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT s.tup.a FROM (SELECT tup FROM t_push_subcolumns_partial) AS s) WHERE explain LIKE '%Output%';
SELECT s.tup.a FROM (SELECT tup FROM t_push_subcolumns_partial) AS s ORDER BY 1;

SELECT 'the pushable subcolumn alone is pushed';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT s.tup.b FROM (SELECT tup FROM t_push_subcolumns_partial) AS s) WHERE explain LIKE '%Output%';
SELECT s.tup.b FROM (SELECT tup FROM t_push_subcolumns_partial) AS s ORDER BY 1;

DROP TABLE t_push_subcolumns_partial;
