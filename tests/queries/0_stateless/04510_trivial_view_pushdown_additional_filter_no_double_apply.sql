-- A view-keyed additional_table_filters predicate is folded into the outer
-- query's WHERE and shipped to shards when the trivial-view pushdown fires
-- (PlannerJoinTree.cpp: the additional_filter_ast merge). Separately, an
-- earlier, unconditional buildAdditionalFiltersIfNeeded() call has already
-- added a coordinator-side FilterStep for the SAME predicate to `where_filters`
-- (described as "additional filter"), meant for the normal, non-pushdown path.
-- That where_filters entry is never explicitly removed when the fold happens,
-- so it could in principle be applied a second time on the coordinator on top
-- of the already-shipped, already-filtered shard result.
--
-- In practice this does not happen: the where_filters loop only turns an
-- entry into a real FilterStep when till_stage == FetchColumns, and once
-- pushdown swaps effective_storage to the underlying Distributed table,
-- till_stage is recomputed from StorageDistributed::getQueryProcessingStage,
-- which never returns FetchColumns for a real cluster (>= 1 shard) -- so the
-- leftover where_filters entry is inert. This test locks that in: the
-- "additional filter" FilterStep must NOT appear a second time in the plan
-- once pushdown has already shipped the predicate to the shard.
--
-- Tags: distributed

SET enable_analyzer = 1;
-- Pin the legacy EXPLAIN plan format: the assertions below grep step
-- descriptions ("additional filter", "VIEW subquery"), which the default
-- 'pretty' EXPLAIN format (explain_query_plan_default) does not print.
SET explain_query_plan_default = 'legacy';
SET prefer_localhost_replica = 0;

DROP TABLE IF EXISTS 04510_local;
DROP TABLE IF EXISTS 04510_dist;
DROP VIEW IF EXISTS 04510_view;

CREATE TABLE 04510_local (id UInt32) ENGINE = MergeTree ORDER BY id;

CREATE TABLE 04510_dist AS 04510_local
ENGINE = Distributed(test_shard_localhost, currentDatabase(), 04510_local);

INSERT INTO 04510_dist VALUES (1), (2), (3), (4);
SYSTEM FLUSH DISTRIBUTED 04510_dist;

CREATE VIEW 04510_view AS SELECT id FROM 04510_dist;

-- Baseline, pushdown OFF: the normal path applies the additional filter as a
-- coordinator-side "additional filter" FilterStep -- proves the marker this
-- test greps for is meaningful (not vacuously absent).
SELECT countIf(explain LIKE '%additional filter%') > 0 AS coordinator_filter_present
FROM (EXPLAIN SELECT id FROM 04510_view
      SETTINGS additional_table_filters = {'04510_view': 'id % 2 = 0'}, optimize_trivial_view_pushdown_to_distributed = 0);

SET optimize_trivial_view_pushdown_to_distributed = 1;

-- Pushdown fires for this view/query (no "VIEW subquery" steps) ...
SELECT countIf(explain LIKE '%VIEW subquery%') = 0 AS pushdown_fires
FROM (EXPLAIN SELECT id FROM 04510_view
      SETTINGS additional_table_filters = {'04510_view': 'id % 2 = 0'});

-- ... and the leftover where_filters "additional filter" entry must NOT also
-- appear as a second, coordinator-side FilterStep: the predicate was already
-- folded into the shipped query, so applying it again here would mean it ran
-- twice.
SELECT countIf(explain LIKE '%additional filter%') = 0 AS no_duplicate_coordinator_filter
FROM (EXPLAIN SELECT id FROM 04510_view
      SETTINGS additional_table_filters = {'04510_view': 'id % 2 = 0'});

-- Result correctness, identical with the setting on and off: only even ids.
SELECT id FROM 04510_view ORDER BY id
SETTINGS additional_table_filters = {'04510_view': 'id % 2 = 0'};
SET optimize_trivial_view_pushdown_to_distributed = 0;
SELECT id FROM 04510_view ORDER BY id
SETTINGS additional_table_filters = {'04510_view': 'id % 2 = 0'};
SET optimize_trivial_view_pushdown_to_distributed = 1;

DROP VIEW 04510_view;
DROP TABLE 04510_dist;
DROP TABLE 04510_local;
