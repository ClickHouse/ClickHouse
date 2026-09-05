-- tryGetTrivialViewUnderlyingStorage rejects a view whose own projection contains an aggregate,
-- a window function, or a scalar subquery (hasAggregate(expr) / hasSubqueryOrWindow(expr) per
-- SELECT-list expression), so such a view is never considered "trivial" and the
-- optimize_trivial_view_pushdown_to_distributed pushdown never applies to it. 04695 and 04372
-- cover an aggregating *outer* query, but no *trivial_view_pushdown* test defines a
-- non-trivial view body like this. If that guard ever regressed, each shard could execute the
-- full outer query independently and emit partial aggregate/window/subquery results instead of
-- a single value computed once on the initiator, and none of the existing tests would notice.
--
-- Tags: distributed

SET enable_analyzer = 1;
-- Pin the legacy EXPLAIN plan format: the pushdown checks below grep for the
-- "Convert VIEW subquery result to VIEW table structure" step, which the default
-- 'pretty' EXPLAIN format (explain_query_plan_default) does not print.
SET explain_query_plan_default = 'legacy';
SET prefer_localhost_replica = 0;

DROP TABLE IF EXISTS 04837_local;
DROP TABLE IF EXISTS 04837_dist;
DROP TABLE IF EXISTS 04837_other;
DROP VIEW IF EXISTS 04837_view_plain;
DROP VIEW IF EXISTS 04837_view_agg;
DROP VIEW IF EXISTS 04837_view_window;
DROP VIEW IF EXISTS 04837_view_subquery;

CREATE TABLE 04837_local (id UInt32, v UInt32) ENGINE = MergeTree ORDER BY id;

CREATE TABLE 04837_dist AS 04837_local
ENGINE = Distributed(test_shard_localhost, currentDatabase(), 04837_local);

-- Initiator-local table referenced by the view-body scalar subquery.
CREATE TABLE 04837_other (m UInt32) ENGINE = MergeTree ORDER BY m;

INSERT INTO 04837_dist VALUES (1, 10), (2, 20), (3, 30);
SYSTEM FLUSH DISTRIBUTED 04837_dist;
INSERT INTO 04837_other VALUES (100);

-- Positive control: a plain trivial view still fires the pushdown, so the assertions below are
-- specific to the non-trivial projection and not vacuously true.
CREATE VIEW 04837_view_plain AS SELECT id, v FROM 04837_dist;
SELECT countIf(explain LIKE '%VIEW subquery%') = 0 AS pushdown_fires
FROM (EXPLAIN SELECT id, v FROM 04837_view_plain SETTINGS optimize_trivial_view_pushdown_to_distributed = 1);

-- The view body's own projection has an aggregate: the view is not trivial, pushdown never fires.
CREATE VIEW 04837_view_agg AS SELECT sum(v) AS s FROM 04837_dist;
SELECT countIf(explain LIKE '%VIEW subquery%') > 0 AS pushdown_suppressed
FROM (EXPLAIN SELECT s FROM 04837_view_agg SETTINGS optimize_trivial_view_pushdown_to_distributed = 1);

-- The view body's own projection has a window function.
CREATE VIEW 04837_view_window AS SELECT id, sum(v) OVER (ORDER BY id) AS running_sum FROM 04837_dist;
SELECT countIf(explain LIKE '%VIEW subquery%') > 0 AS pushdown_suppressed
FROM (EXPLAIN SELECT running_sum FROM 04837_view_window SETTINGS optimize_trivial_view_pushdown_to_distributed = 1);

-- The view body's own projection has a scalar subquery.
CREATE VIEW 04837_view_subquery AS SELECT id, (SELECT max(m) FROM 04837_other) AS x FROM 04837_dist;
SELECT countIf(explain LIKE '%VIEW subquery%') > 0 AS pushdown_suppressed
FROM (EXPLAIN SELECT x FROM 04837_view_subquery SETTINGS optimize_trivial_view_pushdown_to_distributed = 1);

-- Correctness: identical results whether the setting is on or off.
SELECT s FROM 04837_view_agg SETTINGS optimize_trivial_view_pushdown_to_distributed = 1;
SELECT s FROM 04837_view_agg SETTINGS optimize_trivial_view_pushdown_to_distributed = 0;
SELECT id, running_sum FROM 04837_view_window ORDER BY id SETTINGS optimize_trivial_view_pushdown_to_distributed = 1;
SELECT id, running_sum FROM 04837_view_window ORDER BY id SETTINGS optimize_trivial_view_pushdown_to_distributed = 0;
SELECT id, x FROM 04837_view_subquery ORDER BY id SETTINGS optimize_trivial_view_pushdown_to_distributed = 1;
SELECT id, x FROM 04837_view_subquery ORDER BY id SETTINGS optimize_trivial_view_pushdown_to_distributed = 0;

DROP VIEW 04837_view_plain;
DROP VIEW 04837_view_agg;
DROP VIEW 04837_view_window;
DROP VIEW 04837_view_subquery;
DROP TABLE 04837_dist;
DROP TABLE 04837_local;
DROP TABLE 04837_other;
