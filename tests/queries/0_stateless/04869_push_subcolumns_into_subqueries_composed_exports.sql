-- Tags: no-parallel-replicas, no-random-settings
-- The test checks EXPLAIN output, which differs with parallel replicas and randomized plan-related settings.

SET enable_analyzer = 1;
SET optimize_push_subcolumns_into_subqueries = 1;

DROP TABLE IF EXISTS t_push_subcolumns_composed;

CREATE TABLE t_push_subcolumns_composed (id UInt32, tup Tuple(a Tuple(b UInt32, c String), d UInt32))
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_push_subcolumns_composed VALUES (1, ((1, 'one'), 10)), (2, ((2, 'two'), 20));

-- A subquery can export a derived subcolumn: over a deeper subquery `tup.a AS x` stays a
-- `getSubcolumn(tup, 'a')` projection expression. Reading a subcolumn of such an export is
-- reading a deeper subcolumn of the underlying column, so the paths compose (`a` + `b` -> `a.b`)
-- and the pushdown continues through the derived export down to the base table.

SELECT 'derived export over a subquery, paths composed down to the table';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT x.b FROM (SELECT tup.a AS x FROM (SELECT tup FROM t_push_subcolumns_composed))) WHERE explain LIKE '%Output%';
SELECT x.b FROM (SELECT tup.a AS x FROM (SELECT tup FROM t_push_subcolumns_composed)) ORDER BY x.b;

SELECT 'derived export directly over the table';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT x.b FROM (SELECT tup.a AS x FROM t_push_subcolumns_composed)) WHERE explain LIKE '%Output%';
SELECT x.b FROM (SELECT tup.a AS x FROM t_push_subcolumns_composed) ORDER BY x.b;

SELECT 'multi-element path composed through the derived export';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT x.a.c FROM (SELECT tup AS x FROM (SELECT tup FROM t_push_subcolumns_composed))) WHERE explain LIKE '%Output%';
SELECT x.a.c FROM (SELECT tup AS x FROM (SELECT tup FROM t_push_subcolumns_composed)) ORDER BY x.a.c;

SELECT 'derived export through a CTE';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 WITH middle AS (SELECT tup.a AS x FROM (SELECT tup FROM t_push_subcolumns_composed)) SELECT x.b FROM middle) WHERE explain LIKE '%Output%';
WITH middle AS (SELECT tup.a AS x FROM (SELECT tup FROM t_push_subcolumns_composed)) SELECT x.b FROM middle ORDER BY x.b;

SELECT 'derived export also read whole, the parent subcolumn is still read';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT x.b, x FROM (SELECT tup.a AS x FROM (SELECT tup FROM t_push_subcolumns_composed))) WHERE explain LIKE '%Output%';
SELECT x.b, x FROM (SELECT tup.a AS x FROM (SELECT tup FROM t_push_subcolumns_composed)) ORDER BY x.b;

SELECT 'composition disabled with the setting';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT x.b FROM (SELECT tup.a AS x FROM (SELECT tup FROM t_push_subcolumns_composed)) SETTINGS optimize_push_subcolumns_into_subqueries = 0) WHERE explain LIKE '%Output%';

DROP TABLE t_push_subcolumns_composed;

DROP TABLE IF EXISTS t_push_subcolumns_composed_json;

CREATE TABLE t_push_subcolumns_composed_json (id UInt32, json JSON(a Tuple(b UInt32, c String)))
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_push_subcolumns_composed_json VALUES (1, '{"a": {"b": 1, "c": "one"}}'), (2, '{"a": {"b": 2, "c": "two"}}');

SELECT 'JSON derived export over a subquery, paths composed down to the table';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT x.b FROM (SELECT json.a AS x FROM (SELECT json FROM t_push_subcolumns_composed_json))) WHERE explain LIKE '%Output%';
SELECT x.b FROM (SELECT json.a AS x FROM (SELECT json FROM t_push_subcolumns_composed_json)) ORDER BY x.b;

DROP TABLE t_push_subcolumns_composed_json;
