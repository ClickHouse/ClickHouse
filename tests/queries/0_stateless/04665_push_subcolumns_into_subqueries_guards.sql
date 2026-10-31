-- Tags: no-parallel-replicas, no-random-settings
-- The test checks EXPLAIN output, which differs with parallel replicas and randomized plan-related settings.

SET enable_analyzer = 1;
SET optimize_push_subcolumns_into_subqueries = 1;

DROP TABLE IF EXISTS t_push_subcolumns_guards;

CREATE TABLE t_push_subcolumns_guards (id UInt32, json JSON, n Nullable(UInt32))
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_push_subcolumns_guards VALUES (1, '{"a": 1, "b": "x"}', 1), (2, '{"a": 2, "b": "y"}', NULL);

SELECT 'setting disabled inside the subquery';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT json.a FROM (SELECT * FROM t_push_subcolumns_guards SETTINGS optimize_push_subcolumns_into_subqueries = 0)) WHERE explain LIKE '%Output%';
SELECT json.a FROM (SELECT * FROM t_push_subcolumns_guards SETTINGS optimize_push_subcolumns_into_subqueries = 0) ORDER BY id;

SELECT 'setting disabled inside the CTE';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 WITH s AS (SELECT * FROM t_push_subcolumns_guards SETTINGS optimize_push_subcolumns_into_subqueries = 0) SELECT json.a FROM s) WHERE explain LIKE '%Output%';
WITH s AS (SELECT * FROM t_push_subcolumns_guards SETTINGS optimize_push_subcolumns_into_subqueries = 0) SELECT json.a FROM s ORDER BY id;

SELECT 'setting disabled in the middle of nested subqueries';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT json.a FROM (SELECT * FROM (SELECT * FROM t_push_subcolumns_guards) SETTINGS optimize_push_subcolumns_into_subqueries = 0)) WHERE explain LIKE '%Output%';
SELECT json.a FROM (SELECT * FROM (SELECT * FROM t_push_subcolumns_guards) SETTINGS optimize_push_subcolumns_into_subqueries = 0) ORDER BY id;

SELECT 'storage that does not support the optimization to subcolumns';
INSERT INTO FUNCTION file(currentDatabase() || '_04665_guards.jsonl', JSONEachRow, 'id UInt32, n Nullable(UInt32)')
SELECT id, n FROM t_push_subcolumns_guards SETTINGS engine_file_truncate_on_insert = 1;

SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT n.null FROM (SELECT * FROM file(currentDatabase() || '_04665_guards.jsonl', JSONEachRow, 'id UInt32, n Nullable(UInt32)'))) WHERE explain LIKE '%Output%';
SELECT n.null FROM (SELECT * FROM file(currentDatabase() || '_04665_guards.jsonl', JSONEachRow, 'id UInt32, n Nullable(UInt32)')) ORDER BY id;

DROP TABLE t_push_subcolumns_guards;
