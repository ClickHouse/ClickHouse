-- A view may declare an explicit column schema whose types differ from the inner
-- query's result types (e.g. CREATE VIEW v (id UInt8) AS SELECT id FROM dist where
-- dist.id is UInt32). StorageView::readImpl converts the inner result to the view's
-- declared structure; the trivial-view pushdown ships the raw inner query and skips
-- that conversion, so a shard would return the inner types and break the VIEW type
-- contract. The pushdown must be suppressed whenever any read column's declared type
-- differs from the inner output type. This test asserts the declared type is honoured
-- and the result is identical with the setting on and off.
--
-- Tags: distributed

SET enable_analyzer = 1;
-- Pin the legacy EXPLAIN plan format: the pushdown checks below grep for the
-- "Convert VIEW subquery result to VIEW table structure" step, which the default
-- 'pretty' EXPLAIN format (explain_query_plan_default) does not print.
SET explain_query_plan_default = 'legacy';
SET enable_parallel_replicas = 0;
SET prefer_localhost_replica = 0;

DROP TABLE IF EXISTS 04370_local;
DROP TABLE IF EXISTS 04370_dist;
DROP VIEW IF EXISTS 04370_view;

CREATE TABLE 04370_local (id UInt32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE 04370_dist AS 04370_local
ENGINE = Distributed(test_shard_localhost, currentDatabase(), 04370_local);

-- The view narrows the underlying UInt32 to a declared UInt8.
CREATE VIEW 04370_view (id UInt8) AS SELECT id FROM 04370_dist;

INSERT INTO 04370_dist VALUES (1), (2);
SYSTEM FLUSH DISTRIBUTED 04370_dist;

-- The declared type must win in both modes, and the pushdown must be suppressed
-- (plan keeps the "VIEW subquery" steps) because the inner type (UInt32) differs.
SET optimize_trivial_view_pushdown_to_distributed = 1;
SELECT countIf(explain LIKE '%VIEW subquery%') > 0 AS pushdown_suppressed
FROM (EXPLAIN SELECT id FROM 04370_view);

SELECT toTypeName(id), id FROM 04370_view ORDER BY id;
SET optimize_trivial_view_pushdown_to_distributed = 0;
SELECT toTypeName(id), id FROM 04370_view ORDER BY id;

-- A view whose declared type matches the inner type is still eligible for the pushdown.
DROP VIEW 04370_view;
CREATE VIEW 04370_view (id UInt32) AS SELECT id FROM 04370_dist;
SET optimize_trivial_view_pushdown_to_distributed = 1;
SELECT countIf(explain LIKE '%VIEW subquery%') = 0 AS pushdown_fires
FROM (EXPLAIN SELECT id FROM 04370_view);

DROP VIEW 04370_view;
DROP TABLE 04370_dist;
DROP TABLE 04370_local;
