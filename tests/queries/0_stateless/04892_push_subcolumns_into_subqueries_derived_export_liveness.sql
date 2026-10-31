-- Tags: no-parallel-replicas, no-random-settings
-- The test checks EXPLAIN output, which differs with parallel replicas and randomized plan-related settings.

SET enable_analyzer = 1;
SET optimize_push_subcolumns_into_subqueries = 1;

DROP TABLE IF EXISTS t_push_subcolumns_derived_liveness;

CREATE TABLE t_push_subcolumns_derived_liveness (id UInt32, tup Tuple(a Tuple(b UInt32, c String), d UInt32))
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_push_subcolumns_derived_liveness VALUES (1, ((1, 'one'), 10)), (2, ((2, 'two'), 20));

-- A derived subcolumn export (`tup.a AS x`) is a part of the whole column `tup`: while `tup` stays
-- alive under any exported name, the table reads the whole column anyway, and pushing `x.b` down
-- would only add a second read of `tup.a.b` to it.

SELECT 'whole column alive next to a derived subcolumn export, not pushed';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT x.b, tup FROM (SELECT tup.a AS x, tup FROM t_push_subcolumns_derived_liveness)) WHERE explain LIKE '%Output%';
SELECT x.b, tup FROM (SELECT tup.a AS x, tup FROM t_push_subcolumns_derived_liveness) ORDER BY x.b;

SELECT 'whole column alive under an alias name, not pushed';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT x.b, y FROM (SELECT tup.a AS x, tup AS y FROM t_push_subcolumns_derived_liveness)) WHERE explain LIKE '%Output%';
SELECT x.b, y FROM (SELECT tup.a AS x, tup AS y FROM t_push_subcolumns_derived_liveness) ORDER BY x.b;

SELECT 'the derived export itself alive next to its subcolumn, not pushed';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT x.b, x FROM (SELECT tup.a AS x FROM t_push_subcolumns_derived_liveness)) WHERE explain LIKE '%Output%';
SELECT x.b, x FROM (SELECT tup.a AS x FROM t_push_subcolumns_derived_liveness) ORDER BY x.b;

-- A sibling export that reads another part of the same column does not keep the pushed subcolumn
-- alive: both are narrower reads than the whole column.

SELECT 'sibling subcolumn export alive, pushed';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT x.b, d FROM (SELECT tup.a AS x, tup.d AS d FROM t_push_subcolumns_derived_liveness)) WHERE explain LIKE '%Output%';
SELECT x.b, d FROM (SELECT tup.a AS x, tup.d AS d FROM t_push_subcolumns_derived_liveness) ORDER BY x.b;

SELECT 'whole column exported but read only through subcolumns, pushed';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT x.b, tup.d FROM (SELECT tup.a AS x, tup FROM t_push_subcolumns_derived_liveness)) WHERE explain LIKE '%Output%';
SELECT x.b, tup.d FROM (SELECT tup.a AS x, tup FROM t_push_subcolumns_derived_liveness) ORDER BY x.b;

SELECT 'derived export alone, pushed';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT x.b FROM (SELECT tup.a AS x, tup FROM t_push_subcolumns_derived_liveness)) WHERE explain LIKE '%Output%';
SELECT x.b FROM (SELECT tup.a AS x, tup FROM t_push_subcolumns_derived_liveness) ORDER BY x.b;

-- The same shapes through two levels of subqueries: the derived export of the middle query keeps a
-- `getSubcolumn` projection expression over the deeper subquery.

SELECT 'two levels, whole column alive in the middle query, not pushed';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT x.b, tup FROM (SELECT tup.a AS x, tup FROM (SELECT tup FROM t_push_subcolumns_derived_liveness))) WHERE explain LIKE '%Output%';
SELECT x.b, tup FROM (SELECT tup.a AS x, tup FROM (SELECT tup FROM t_push_subcolumns_derived_liveness)) ORDER BY x.b;

SELECT 'two levels, whole column not exported by the middle query, pushed to the table';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT x.b FROM (SELECT tup.a AS x FROM (SELECT tup FROM t_push_subcolumns_derived_liveness))) WHERE explain LIKE '%Output%';
SELECT x.b FROM (SELECT tup.a AS x FROM (SELECT tup FROM t_push_subcolumns_derived_liveness)) ORDER BY x.b;

-- The whole column exported by the middle query next to the derived export is never referenced.
-- It is pruned together with the derived export, so it does not stop the pushdown to the table.
SELECT 'two levels, unreferenced whole column exported by the middle query, pushed to the table';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT x.b FROM (SELECT tup.a AS x, tup FROM (SELECT tup FROM t_push_subcolumns_derived_liveness))) WHERE explain LIKE '%Output%';
SELECT x.b FROM (SELECT tup.a AS x, tup FROM (SELECT tup FROM t_push_subcolumns_derived_liveness)) ORDER BY x.b;

SELECT 'setting off, not pushed';
SELECT x.b, tup FROM (SELECT tup.a AS x, tup FROM t_push_subcolumns_derived_liveness) ORDER BY x.b SETTINGS optimize_push_subcolumns_into_subqueries = 0;

DROP TABLE t_push_subcolumns_derived_liveness;
