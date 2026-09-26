-- `_table`/`_database` are materialized by StorageView's
-- StorageWithCommonVirtualColumns::read as constants equal to the view's own
-- name, not the underlying table's. The trivial-view pushdown ships the inner
-- query against the underlying Distributed table directly, bypassing that
-- materialization. Before the fix this either threw UNKNOWN_IDENTIFIER (the
-- re-analyzed inner query has no `_table`/`_database` column in scope) or
-- would have resolved to the wrong (shard-local) name. The pushdown must be
-- suppressed whenever the outer query reads either virtual column from the
-- view, falling back to readImpl.
--
-- Tags: distributed

SET enable_analyzer = 1;
-- Pin the legacy EXPLAIN plan format: the pushdown checks below grep for the
-- "Convert VIEW subquery result to VIEW table structure" step, which the default
-- 'pretty' EXPLAIN format (explain_query_plan_default) does not print.
SET explain_query_plan_default = 'legacy';
SET prefer_localhost_replica = 0;

DROP TABLE IF EXISTS 04507_local;
DROP TABLE IF EXISTS 04507_dist;
DROP VIEW IF EXISTS 04507_view;

CREATE TABLE 04507_local (id UInt32) ENGINE = MergeTree ORDER BY id;

CREATE TABLE 04507_dist AS 04507_local
ENGINE = Distributed(test_shard_localhost, currentDatabase(), 04507_local);

INSERT INTO 04507_dist VALUES (1), (2), (3);
SYSTEM FLUSH DISTRIBUTED 04507_dist;

CREATE VIEW 04507_view AS SELECT id FROM 04507_dist;

SET optimize_trivial_view_pushdown_to_distributed = 1;

-- Baseline: pushdown fires for a query that does not read `_table`/`_database`.
SELECT countIf(explain LIKE '%VIEW subquery%') = 0 AS pushdown_fires
FROM (EXPLAIN SELECT id FROM 04507_view);

-- `_table` in the projection: pushdown must be suppressed (plan keeps the
-- "VIEW subquery" steps) ...
SELECT countIf(explain LIKE '%VIEW subquery%') > 0 AS pushdown_suppressed
FROM (EXPLAIN SELECT _table FROM 04507_view);

-- ... and it must resolve to the view's own name, identical with the setting
-- on and off (previously threw UNKNOWN_IDENTIFIER with the setting on).
SELECT _table FROM 04507_view LIMIT 1;
SET optimize_trivial_view_pushdown_to_distributed = 0;
SELECT _table FROM 04507_view LIMIT 1;
SET optimize_trivial_view_pushdown_to_distributed = 1;

-- `_database` in the projection: likewise suppressed and correct.
SELECT countIf(explain LIKE '%VIEW subquery%') > 0 AS pushdown_suppressed
FROM (EXPLAIN SELECT _database FROM 04507_view);

SELECT _database FROM 04507_view LIMIT 1;
SET optimize_trivial_view_pushdown_to_distributed = 0;
SELECT _database FROM 04507_view LIMIT 1;
SET optimize_trivial_view_pushdown_to_distributed = 1;

-- `_table` referenced only in the WHERE filter (not the projection): pushdown
-- must still be suppressed, and the filter must correctly match the view's
-- own name (previously threw UNKNOWN_IDENTIFIER with the setting on).
SELECT countIf(explain LIKE '%VIEW subquery%') > 0 AS pushdown_suppressed
FROM (EXPLAIN SELECT id FROM 04507_view WHERE _table = '04507_view');

SELECT id FROM 04507_view WHERE _table = '04507_view' ORDER BY id;
SET optimize_trivial_view_pushdown_to_distributed = 0;
SELECT id FROM 04507_view WHERE _table = '04507_view' ORDER BY id;
SET optimize_trivial_view_pushdown_to_distributed = 1;

-- A query that reads `_table` from the *underlying* Distributed table directly
-- (not through the view) is unaffected: it must resolve to the Distributed
-- table's own name.
SELECT _table FROM 04507_dist LIMIT 1;

DROP VIEW 04507_view;
DROP TABLE 04507_dist;
DROP TABLE 04507_local;
