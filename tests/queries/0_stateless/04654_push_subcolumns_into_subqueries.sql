-- Tags: no-parallel-replicas, no-random-settings
-- The test checks EXPLAIN output, which differs with parallel replicas and randomized plan-related settings.

SET enable_analyzer = 1;
SET optimize_push_subcolumns_into_subqueries = 1;
SET allow_suspicious_types_in_group_by = 1;
SET allow_suspicious_types_in_order_by = 1;

DROP TABLE IF EXISTS t_push_subcolumns;

CREATE TABLE t_push_subcolumns (id UInt32, json JSON, tup Tuple(a UInt32, b String), n Nullable(UInt32), arr Array(UInt32))
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_push_subcolumns VALUES (1, '{"a": 1, "b": "x", "c": {"d": 10}}', (1, 'one'), 1, [1, 2, 3]), (2, '{"a": 2, "b": "y"}', (2, 'two'), NULL, []);

SELECT 'subquery';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT json.a, json.b FROM (SELECT * FROM t_push_subcolumns)) WHERE explain LIKE '%Output%';
SELECT json.a, json.b FROM (SELECT * FROM t_push_subcolumns) ORDER BY id;
SELECT json.a, json.b FROM (SELECT * FROM t_push_subcolumns) ORDER BY id SETTINGS optimize_push_subcolumns_into_subqueries = 0;

SELECT 'subquery, disabled optimization';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT json.a, json.b FROM (SELECT * FROM t_push_subcolumns) SETTINGS optimize_push_subcolumns_into_subqueries = 0) WHERE explain LIKE '%Output%';

SELECT 'nested JSON path';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT json.c.d FROM (SELECT * FROM t_push_subcolumns)) WHERE explain LIKE '%Output%';
SELECT json.c.d FROM (SELECT * FROM t_push_subcolumns) ORDER BY id;

SELECT 'CTE';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 WITH s AS (SELECT * FROM t_push_subcolumns) SELECT json.a FROM s) WHERE explain LIKE '%Output%';
WITH s AS (SELECT * FROM t_push_subcolumns) SELECT json.a FROM s ORDER BY id;

SELECT 'two levels of subqueries';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT json.a FROM (SELECT * FROM (SELECT * FROM t_push_subcolumns))) WHERE explain LIKE '%Output%';
SELECT json.a FROM (SELECT * FROM (SELECT * FROM t_push_subcolumns)) ORDER BY id;

SELECT 'view';
DROP TABLE IF EXISTS v_push_subcolumns;
CREATE VIEW v_push_subcolumns AS SELECT * FROM t_push_subcolumns;
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT json.a FROM v_push_subcolumns) WHERE explain LIKE '%Output%';
SELECT json.a FROM v_push_subcolumns ORDER BY id;
DROP TABLE v_push_subcolumns;

SELECT 'Tuple and Array subcolumns';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT tup.a, tup.b, arr.size0 FROM (SELECT * FROM t_push_subcolumns)) WHERE explain LIKE '%Output%';
SELECT tup.a, tup.b, arr.size0 FROM (SELECT * FROM t_push_subcolumns) ORDER BY id;

SELECT 'Nullable null subcolumn without JOIN';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT n.null FROM (SELECT * FROM t_push_subcolumns)) WHERE explain LIKE '%Output%';
SELECT n.null FROM (SELECT * FROM t_push_subcolumns) ORDER BY id;

SELECT 'whole column and subcolumn';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT json, json.a FROM (SELECT * FROM t_push_subcolumns)) WHERE explain LIKE '%Output%';
SELECT json, json.a FROM (SELECT * FROM t_push_subcolumns) ORDER BY id;

SELECT 'subcolumn in WHERE';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT id FROM (SELECT * FROM t_push_subcolumns) WHERE json.a = 1) WHERE explain LIKE '%Output%';
SELECT id FROM (SELECT * FROM t_push_subcolumns) WHERE json.a = 1;

SELECT 'subcolumn as GROUP BY key';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT json.a FROM (SELECT * FROM t_push_subcolumns) GROUP BY json.a) WHERE explain LIKE '%Output%';
SELECT json.a FROM (SELECT * FROM t_push_subcolumns) GROUP BY json.a ORDER BY json.a;

SELECT 'subcolumn in aggregate function';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT max(tup.a) FROM (SELECT * FROM t_push_subcolumns)) WHERE explain LIKE '%Output%';
SELECT max(tup.a) FROM (SELECT * FROM t_push_subcolumns);

SELECT 'subcolumn of a GROUP BY key is not pushed down';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT tup.a FROM (SELECT * FROM t_push_subcolumns) GROUP BY tup) WHERE explain LIKE '%Output%';
SELECT tup.a FROM (SELECT * FROM t_push_subcolumns) GROUP BY tup ORDER BY tup.a;

SELECT 'not pushed into DISTINCT subquery';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT json.a FROM (SELECT DISTINCT * FROM t_push_subcolumns)) WHERE explain LIKE '%Output%';
SELECT json.a FROM (SELECT DISTINCT * FROM t_push_subcolumns) ORDER BY json.a;

SELECT 'pushed into UNION ALL';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT json.a FROM (SELECT * FROM t_push_subcolumns UNION ALL SELECT * FROM t_push_subcolumns)) WHERE explain LIKE '%Output%';
SELECT json.a FROM (SELECT * FROM t_push_subcolumns UNION ALL SELECT * FROM t_push_subcolumns) ORDER BY json.a;

SELECT 'not pushed into aggregating subquery';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT tup.a FROM (SELECT any(tup) AS tup FROM t_push_subcolumns)) WHERE explain LIKE '%Output%';
SELECT tup.a FROM (SELECT any(tup) AS tup FROM t_push_subcolumns);

SELECT 'pushed for INNER JOIN';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT l.json.a, r.tup.a FROM (SELECT * FROM t_push_subcolumns) l INNER JOIN (SELECT * FROM t_push_subcolumns) r ON l.id = r.id) WHERE explain LIKE '%Output%';
SELECT l.json.a, r.tup.a FROM (SELECT * FROM t_push_subcolumns) l INNER JOIN (SELECT * FROM t_push_subcolumns) r ON l.id = r.id ORDER BY l.id;

SELECT 'not pushed to the right side of LEFT JOIN';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT l.id, r.n.null FROM (SELECT 3 AS id) l LEFT JOIN (SELECT * FROM t_push_subcolumns) r ON l.id = r.id) WHERE explain LIKE '%Output%';
SELECT l.id, r.n.null FROM (SELECT 3 AS id) l LEFT JOIN (SELECT * FROM t_push_subcolumns) r ON l.id = r.id;

DROP TABLE t_push_subcolumns;
