-- Regression test for trivial-view pushdown to Distributed tables
-- (optimize_trivial_view_pushdown_to_distributed) under serialize_query_plan.
--
-- When the outer query over a trivial view is itself lowered to a logical plan
-- (build_logical_plan, set for the shard-side plan under serialize_query_plan = 1),
-- the pushdown is suppressed and the query falls back to StorageView::readImpl.
-- Results must be identical to the non-serialized path.
-- Tags: distributed

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET optimize_trivial_view_pushdown_to_distributed = 1;
SET prefer_localhost_replica = 0;

DROP TABLE IF EXISTS 04623_local;
DROP TABLE IF EXISTS 04623_dist;
DROP VIEW IF EXISTS 04623_view;

CREATE TABLE 04623_local (id UInt32, val UInt32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE 04623_dist AS 04623_local
ENGINE = Distributed(test_shard_localhost, currentDatabase(), 04623_local, id);
CREATE VIEW 04623_view AS SELECT id, val FROM 04623_dist;

INSERT INTO 04623_local SELECT number, number * 10 FROM numbers(10);

-- Baseline: pushdown on the initiator (serialize_query_plan = 0).
SELECT id, val FROM 04623_view WHERE val > 30 ORDER BY id SETTINGS serialize_query_plan = 0;

-- Same query with serialize_query_plan = 1: the shard-side plan is built with
-- build_logical_plan = true, which suppresses the pushdown; the result is unchanged.
SELECT id, val FROM 04623_view WHERE val > 30 ORDER BY id SETTINGS serialize_query_plan = 1;

-- A plain projection with serialize_query_plan = 1.
SELECT sum(val) FROM 04623_view SETTINGS serialize_query_plan = 1;

DROP VIEW 04623_view;
DROP TABLE 04623_dist;
DROP TABLE 04623_local;
