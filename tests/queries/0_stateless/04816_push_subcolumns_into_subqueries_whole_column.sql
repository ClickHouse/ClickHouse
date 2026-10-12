-- Tags: no-parallel-replicas, no-random-settings
-- The test checks EXPLAIN output, which differs with parallel replicas and randomized plan-related settings.

SET enable_analyzer = 1;
SET optimize_push_subcolumns_into_subqueries = 1;

DROP TABLE IF EXISTS t_push_subcolumns_whole;

CREATE TABLE t_push_subcolumns_whole (id UInt32, tup Tuple(a UInt32, b String))
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_push_subcolumns_whole VALUES (1, (1, 'one')), (2, (2, 'two'));

-- When the whole column is still used in the outer query, the subcolumn is not pushed down:
-- otherwise the subquery would read both the whole column and the subcolumn from the table,
-- while extracting the subcolumn from the already read column is cheaper.

SELECT 'whole column in the projection';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT tup, tup.a FROM (SELECT tup FROM t_push_subcolumns_whole)) WHERE explain LIKE '%Output%';
SELECT tup, tup.a FROM (SELECT tup FROM t_push_subcolumns_whole) ORDER BY tup.a;

SELECT 'whole column in WHERE, subcolumn in the projection';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT tup.a FROM (SELECT tup FROM t_push_subcolumns_whole) WHERE tup = (1, 'one')) WHERE explain LIKE '%Output%';
SELECT tup.a FROM (SELECT tup FROM t_push_subcolumns_whole) WHERE tup = (1, 'one');

SELECT 'only subcolumns are used, pushed down';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT tup.a, tup.b FROM (SELECT tup FROM t_push_subcolumns_whole)) WHERE explain LIKE '%Output%';
SELECT tup.a, tup.b FROM (SELECT tup FROM t_push_subcolumns_whole) ORDER BY tup.a;

SELECT 'the same subcolumn used twice, pushed down';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT tup.a FROM (SELECT tup FROM t_push_subcolumns_whole) WHERE tup.a = 1) WHERE explain LIKE '%Output%';
SELECT tup.a FROM (SELECT tup FROM t_push_subcolumns_whole) WHERE tup.a = 1;

DROP TABLE t_push_subcolumns_whole;
