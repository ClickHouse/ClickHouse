-- optimize_trivial_view_pushdown_to_distributed must be suppressed when
-- distributed_group_by_no_merge is set. On the normal StorageView path the view advertises
-- FetchColumns, so an outer GROUP BY aggregates on the initiator over rows from all shards and a
-- group key present on several shards yields one merged row. Under the pushdown the underlying
-- Distributed table's getQueryProcessingStage returns a no-merge stage, so each shard aggregates
-- independently and the initiator only concatenates — producing one partial row per shard. The
-- optimization must bail out and fall back to the merging StorageView path.
--
-- Tags: distributed

SET enable_analyzer = 1;
-- Pin the legacy EXPLAIN plan format: the pushdown checks below grep for the
-- "Convert VIEW subquery result to VIEW table structure" step, which the default
-- 'pretty' EXPLAIN format (explain_query_plan_default) does not print.
SET explain_query_plan_default = 'legacy';
SET enable_parallel_replicas = 0;
SET prefer_localhost_replica = 0;

DROP TABLE IF EXISTS 04358_local;
DROP TABLE IF EXISTS 04358_dist;
DROP VIEW IF EXISTS 04358_view;

CREATE TABLE 04358_local (k UInt32, v UInt32) ENGINE = MergeTree ORDER BY k;

-- Two shards, both reading the same local table: querying the Distributed table sees each row on
-- both shards, so the group key k=1 is present on "two shards". A skipped merge is then observable
-- as two partial rows instead of one.
CREATE TABLE 04358_dist AS 04358_local
ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), 04358_local);

CREATE VIEW 04358_view AS SELECT k, v FROM 04358_dist;

INSERT INTO 04358_local VALUES (1, 10), (1, 20);

-- The pushed-down plan must not appear (VIEW subquery steps present) when no_merge is set.
SELECT countIf(explain LIKE '%VIEW subquery%') > 0 AS pushdown_suppressed
FROM (EXPLAIN SELECT k, sum(v) FROM 04358_view GROUP BY k
      SETTINGS optimize_trivial_view_pushdown_to_distributed = 1, distributed_group_by_no_merge = 1);

-- Correctness: each shard sees {(1,10),(1,20)} so the merged sum over both shards is 60. With the
-- bypass the initiator would have concatenated two per-shard partial rows (1,30),(1,30) instead.
SELECT k, sum(v) FROM 04358_view GROUP BY k
SETTINGS optimize_trivial_view_pushdown_to_distributed = 1, distributed_group_by_no_merge = 1;

-- The result must not depend on the optimization setting.
SELECT k, sum(v) FROM 04358_view GROUP BY k
SETTINGS optimize_trivial_view_pushdown_to_distributed = 0, distributed_group_by_no_merge = 1;

-- Sanity: with distributed_group_by_no_merge = 0 the pushdown is allowed and still correct
-- (partial aggregation on shards, merged on the initiator).
SELECT k, sum(v) FROM 04358_view GROUP BY k
SETTINGS optimize_trivial_view_pushdown_to_distributed = 1, distributed_group_by_no_merge = 0;

DROP VIEW 04358_view;
DROP TABLE 04358_dist;
DROP TABLE 04358_local;
