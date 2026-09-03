-- Tags: no-parallel-replicas, no-random-settings
-- The test checks EXPLAIN output, which differs with parallel replicas and randomized plan-related settings.

SET enable_analyzer = 1;
SET optimize_push_subcolumns_into_subqueries = 1;

DROP TABLE IF EXISTS t_push_subcolumns_alias_exports;

CREATE TABLE t_push_subcolumns_alias_exports (id UInt32, tup Tuple(a UInt32, b String), t1 ALIAS tup)
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_push_subcolumns_alias_exports VALUES (1, (1, 'one')), (2, (2, 'two'));

-- The same physical column exported under two names: while the whole column stays alive under
-- any of the names, pushing a subcolumn read through another name would make the subquery read
-- both the whole column and the subcolumn from the table.

SELECT 'whole column alive under its own name next to a projection alias, not pushed';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT x.a, tup FROM (SELECT tup AS x, tup FROM t_push_subcolumns_alias_exports)) WHERE explain LIKE '%Output%';
SELECT x.a, tup FROM (SELECT tup AS x, tup FROM t_push_subcolumns_alias_exports) ORDER BY x.a;

SELECT 'whole column alive next to an exported trivial ALIAS storage column, not pushed';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT t1.a, tup FROM (SELECT t1, tup FROM t_push_subcolumns_alias_exports)) WHERE explain LIKE '%Output%';
SELECT t1.a, tup FROM (SELECT t1, tup FROM t_push_subcolumns_alias_exports) ORDER BY t1.a;

SELECT 'whole column alive under the alias name, subcolumn read under the base name, not pushed';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT tup.a, x FROM (SELECT tup AS x, tup FROM t_push_subcolumns_alias_exports)) WHERE explain LIKE '%Output%';
SELECT tup.a, x FROM (SELECT tup AS x, tup FROM t_push_subcolumns_alias_exports) ORDER BY tup.a;

SELECT 'both exports used only through subcolumns, pushed';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT x.a, tup.b FROM (SELECT tup AS x, tup FROM t_push_subcolumns_alias_exports)) WHERE explain LIKE '%Output%';
SELECT x.a, tup.b FROM (SELECT tup AS x, tup FROM t_push_subcolumns_alias_exports) ORDER BY x.a;

SELECT 'ALIAS export and base export used only through subcolumns, pushed';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT t1.a, tup.b FROM (SELECT t1, tup FROM t_push_subcolumns_alias_exports)) WHERE explain LIKE '%Output%';
SELECT t1.a, tup.b FROM (SELECT t1, tup FROM t_push_subcolumns_alias_exports) ORDER BY t1.a;

DROP TABLE t_push_subcolumns_alias_exports;
