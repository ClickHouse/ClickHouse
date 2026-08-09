-- Tags: no-parallel-replicas, no-random-settings
-- The test checks EXPLAIN output, which differs with parallel replicas and randomized plan-related settings.

SET enable_analyzer = 1;
SET optimize_push_subcolumns_into_subqueries = 1;

DROP TABLE IF EXISTS t_push_subcolumns_dead_alias;

CREATE TABLE t_push_subcolumns_dead_alias (id UInt32, tup Tuple(a UInt32, b String))
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_push_subcolumns_dead_alias VALUES (1, (1, 'one')), (2, (2, 'two'));

-- The middle query exports the same underlying column under two names. When the outer query
-- reads only a subcolumn through one of the names and never references the other, both exported
-- slots are removed by RemoveUnusedProjectionColumnsPass, so the never-referenced sibling must
-- not count as a whole-column use: the pushdown continues into the innermost subquery and the
-- base table reads only the subcolumn.

SELECT 'dead alias-equivalent sibling export, pushed two levels';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT x.a FROM (SELECT tup AS x, tup FROM (SELECT tup FROM t_push_subcolumns_dead_alias))) WHERE explain LIKE '%Output%';
SELECT x.a FROM (SELECT tup AS x, tup FROM (SELECT tup FROM t_push_subcolumns_dead_alias)) ORDER BY x.a;

SELECT 'the same through the base name, sibling alias dead, pushed two levels';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT tup.a FROM (SELECT tup AS x, tup FROM (SELECT tup FROM t_push_subcolumns_dead_alias))) WHERE explain LIKE '%Output%';
SELECT tup.a FROM (SELECT tup AS x, tup FROM (SELECT tup FROM t_push_subcolumns_dead_alias)) ORDER BY tup.a;

SELECT 'the same shape through a CTE, pushed two levels';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 WITH middle AS (SELECT tup AS x, tup FROM (SELECT tup FROM t_push_subcolumns_dead_alias)) SELECT x.a FROM middle) WHERE explain LIKE '%Output%';
WITH middle AS (SELECT tup AS x, tup FROM (SELECT tup FROM t_push_subcolumns_dead_alias)) SELECT x.a FROM middle ORDER BY x.a;

SELECT 'sibling export alive in the outer query, not pushed';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT x.a, tup FROM (SELECT tup AS x, tup FROM (SELECT tup FROM t_push_subcolumns_dead_alias))) WHERE explain LIKE '%Output%';
SELECT x.a, tup FROM (SELECT tup AS x, tup FROM (SELECT tup FROM t_push_subcolumns_dead_alias)) ORDER BY x.a;

DROP TABLE t_push_subcolumns_dead_alias;
