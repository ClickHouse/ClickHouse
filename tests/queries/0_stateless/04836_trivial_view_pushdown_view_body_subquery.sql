-- tryGetTrivialViewUnderlyingStorage rejects a view whose own body's WHERE contains a
-- subquery (select->where() && hasSubquery(select->where())), so such a view is never
-- considered "trivial" and the optimize_trivial_view_pushdown_to_distributed pushdown never
-- applies to it, regardless of where else in the query a subquery could appear. Every other
-- *trivial_view_pushdown* test places its subquery in the outer query, in a view-keyed
-- additional_table_filter, or in a row policy -- none defines the view body itself with a
-- subquery. If that guard ever regressed, the pushdown would start evaluating the view-body
-- subquery once per shard instead of once on the normal StorageView path, and none of the
-- existing tests would notice.
--
-- Tags: distributed

SET enable_analyzer = 1;
-- Pin the legacy EXPLAIN plan format: the pushdown checks below grep for the
-- "Convert VIEW subquery result to VIEW table structure" step, which the default
-- 'pretty' EXPLAIN format (explain_query_plan_default) does not print.
SET explain_query_plan_default = 'legacy';
SET prefer_localhost_replica = 0;

DROP TABLE IF EXISTS 04836_local;
DROP TABLE IF EXISTS 04836_dist;
DROP TABLE IF EXISTS 04836_filter;
DROP VIEW IF EXISTS 04836_view_plain;
DROP VIEW IF EXISTS 04836_view_in;
DROP VIEW IF EXISTS 04836_view_not_in;

CREATE TABLE 04836_local (id UInt32) ENGINE = MergeTree ORDER BY id;

CREATE TABLE 04836_dist AS 04836_local
ENGINE = Distributed(test_shard_localhost, currentDatabase(), 04836_local);

-- Initiator-local table referenced by the view-body subquery.
CREATE TABLE 04836_filter (id UInt32) ENGINE = MergeTree ORDER BY id;

INSERT INTO 04836_dist VALUES (1), (2), (3);
SYSTEM FLUSH DISTRIBUTED 04836_dist;
INSERT INTO 04836_filter VALUES (1), (2);

-- Positive control: a plain trivial view still fires the pushdown, so the assertions below are
-- specific to the view-body subquery and not vacuously true.
CREATE VIEW 04836_view_plain AS SELECT id FROM 04836_dist;
SELECT countIf(explain LIKE '%VIEW subquery%') = 0 AS pushdown_fires
FROM (EXPLAIN SELECT id FROM 04836_view_plain SETTINGS optimize_trivial_view_pushdown_to_distributed = 1);

-- The view body's own WHERE has an IN (subquery): the view is not trivial, pushdown never fires.
CREATE VIEW 04836_view_in AS SELECT id FROM 04836_dist WHERE id IN (SELECT id FROM 04836_filter);
SELECT countIf(explain LIKE '%VIEW subquery%') > 0 AS pushdown_suppressed
FROM (EXPLAIN SELECT id FROM 04836_view_in SETTINGS optimize_trivial_view_pushdown_to_distributed = 1);

-- Same for NOT IN.
CREATE VIEW 04836_view_not_in AS SELECT id FROM 04836_dist WHERE id NOT IN (SELECT id FROM 04836_filter);
SELECT countIf(explain LIKE '%VIEW subquery%') > 0 AS pushdown_suppressed
FROM (EXPLAIN SELECT id FROM 04836_view_not_in SETTINGS optimize_trivial_view_pushdown_to_distributed = 1);

-- Correctness: identical results whether the setting is on or off (04836_dist holds {1, 2, 3};
-- 04836_filter holds {1, 2}).
SELECT id FROM 04836_view_in ORDER BY id SETTINGS optimize_trivial_view_pushdown_to_distributed = 1;
SELECT id FROM 04836_view_in ORDER BY id SETTINGS optimize_trivial_view_pushdown_to_distributed = 0;
SELECT id FROM 04836_view_not_in ORDER BY id SETTINGS optimize_trivial_view_pushdown_to_distributed = 1;
SELECT id FROM 04836_view_not_in ORDER BY id SETTINGS optimize_trivial_view_pushdown_to_distributed = 0;

DROP VIEW 04836_view_plain;
DROP VIEW 04836_view_in;
DROP VIEW 04836_view_not_in;
DROP TABLE 04836_dist;
DROP TABLE 04836_local;
DROP TABLE 04836_filter;
