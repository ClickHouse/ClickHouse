-- Tags: no-parallel-replicas, no-random-settings
-- The test checks EXPLAIN output, which differs with parallel replicas and randomized plan-related settings.

SET enable_analyzer = 1;
SET optimize_push_subcolumns_into_subqueries = 1;
SET allow_suspicious_types_in_group_by = 1;
SET allow_suspicious_types_in_order_by = 1;

DROP TABLE IF EXISTS t_push_union;
DROP TABLE IF EXISTS t_push_union_2;

CREATE TABLE t_push_union (id UInt32, json JSON, tup Tuple(a UInt32, b String))
ENGINE = MergeTree ORDER BY id;

CREATE TABLE t_push_union_2 (id UInt32, json JSON, tup Tuple(a UInt32, b String))
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_push_union VALUES (1, '{"a": 1, "b": "x"}', (1, 'one')), (2, '{"a": 2, "b": "y"}', (2, 'two'));
INSERT INTO t_push_union_2 VALUES (3, '{"a": 3, "b": "z"}', (3, 'three'));

SELECT 'UNION ALL subquery';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT json.a FROM (SELECT json FROM t_push_union UNION ALL SELECT json FROM t_push_union_2)) WHERE explain LIKE '%Output%';
SELECT json.a FROM (SELECT json FROM t_push_union UNION ALL SELECT json FROM t_push_union_2) ORDER BY ALL;
SELECT json.a FROM (SELECT json FROM t_push_union UNION ALL SELECT json FROM t_push_union_2) ORDER BY ALL SETTINGS optimize_push_subcolumns_into_subqueries = 0;

SELECT 'UNION ALL with different column names in branches';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT tup.a FROM (SELECT tup FROM t_push_union UNION ALL SELECT tup AS tup2 FROM t_push_union_2)) WHERE explain LIKE '%Output%';
SELECT tup.a FROM (SELECT tup FROM t_push_union UNION ALL SELECT tup AS tup2 FROM t_push_union_2) ORDER BY ALL;

SELECT 'UNION ALL CTE';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 WITH u AS (SELECT tup FROM t_push_union UNION ALL SELECT tup FROM t_push_union_2) SELECT tup.a, tup.b FROM u) WHERE explain LIKE '%Output%';
WITH u AS (SELECT tup FROM t_push_union UNION ALL SELECT tup FROM t_push_union_2) SELECT tup.a, tup.b FROM u ORDER BY ALL;

SELECT 'view over UNION ALL';
DROP TABLE IF EXISTS v_push_union;
CREATE VIEW v_push_union AS SELECT json FROM t_push_union UNION ALL SELECT json FROM t_push_union_2;
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT json.a FROM v_push_union) WHERE explain LIKE '%Output%';
SELECT json.a FROM v_push_union ORDER BY ALL;
DROP TABLE v_push_union;

SELECT 'nested UNION ALL';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT json.a FROM (SELECT json FROM t_push_union UNION ALL (SELECT json FROM t_push_union_2 UNION ALL SELECT json FROM t_push_union))) WHERE explain LIKE '%Output%';
SELECT json.a FROM (SELECT json FROM t_push_union UNION ALL (SELECT json FROM t_push_union_2 UNION ALL SELECT json FROM t_push_union)) ORDER BY ALL;

SELECT 'two levels of subqueries under UNION ALL';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT json.a FROM (SELECT json FROM (SELECT json FROM t_push_union) UNION ALL SELECT json FROM t_push_union_2)) WHERE explain LIKE '%Output%';
SELECT json.a FROM (SELECT json FROM (SELECT json FROM t_push_union) UNION ALL SELECT json FROM t_push_union_2) ORDER BY ALL;

SELECT 'UNION ALL subquery under a subquery';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT json.a FROM (SELECT json FROM (SELECT json FROM t_push_union UNION ALL SELECT json FROM t_push_union_2))) WHERE explain LIKE '%Output%';
SELECT json.a FROM (SELECT json FROM (SELECT json FROM t_push_union UNION ALL SELECT json FROM t_push_union_2)) ORDER BY ALL;

SELECT 'alias list over UNION ALL';
-- No EXPLAIN here: `FROM (EXPLAIN ...)` loses the `(x)` alias list on the AST roundtrip
-- (a pre-existing quirk, see 04813_push_subcolumns_into_subqueries_alias_list.sh).
SELECT s.x.a FROM (SELECT tup FROM t_push_union UNION ALL SELECT tup FROM t_push_union_2) AS s(x) ORDER BY ALL;

SELECT 'UNION DISTINCT is not rewritten';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT tup.a FROM (SELECT tup FROM t_push_union UNION DISTINCT SELECT tup FROM t_push_union_2)) WHERE explain LIKE '%Output%';
SELECT tup.a FROM (SELECT tup FROM t_push_union UNION DISTINCT SELECT tup FROM t_push_union_2) ORDER BY ALL;

SELECT 'INTERSECT is not rewritten';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT tup.a FROM (SELECT tup FROM t_push_union INTERSECT ALL SELECT tup FROM t_push_union)) WHERE explain LIKE '%Output%';
SELECT tup.a FROM (SELECT tup FROM t_push_union INTERSECT ALL SELECT tup FROM t_push_union) ORDER BY ALL;

SELECT 'whole column and subcolumn';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT tup, tup.a FROM (SELECT tup FROM t_push_union UNION ALL SELECT tup FROM t_push_union_2)) WHERE explain LIKE '%Output%';
SELECT tup, tup.a FROM (SELECT tup FROM t_push_union UNION ALL SELECT tup FROM t_push_union_2) ORDER BY ALL;

SELECT 'diverging branch types are not rewritten';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT tup.a FROM (SELECT tup FROM t_push_union UNION ALL SELECT CAST(tup, 'Tuple(a UInt64, b String)') AS tup FROM t_push_union_2)) WHERE explain LIKE '%Output%';
SELECT tup.a FROM (SELECT tup FROM t_push_union UNION ALL SELECT CAST(tup, 'Tuple(a UInt64, b String)') AS tup FROM t_push_union_2) ORDER BY ALL;

SELECT 'setting disabled in a branch';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT tup.a FROM (SELECT tup FROM t_push_union UNION ALL SELECT tup FROM t_push_union_2 SETTINGS optimize_push_subcolumns_into_subqueries = 0)) WHERE explain LIKE '%Output%';
SELECT tup.a FROM (SELECT tup FROM t_push_union UNION ALL SELECT tup FROM t_push_union_2 SETTINGS optimize_push_subcolumns_into_subqueries = 0) ORDER BY ALL;

SELECT 'UNION ALL on a defaultable JOIN side is not rewritten';
SELECT l.id, r.tup.a FROM (SELECT id FROM t_push_union) AS l LEFT JOIN (SELECT id, tup FROM t_push_union UNION ALL SELECT id, tup FROM t_push_union_2) AS r ON l.id = r.id ORDER BY ALL;

DROP TABLE t_push_union;
DROP TABLE t_push_union_2;
