-- A join runtime filter must reach the parallel-replicas local plan, the same way an ordinary filter
-- does, instead of being stranded above the aggregation as a separate `Filter` step.

DROP TABLE IF EXISTS pr_rf_probe;
DROP TABLE IF EXISTS pr_rf_build;

CREATE TABLE pr_rf_probe (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO pr_rf_probe SELECT number, number FROM numbers(100000);

CREATE TABLE pr_rf_build (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO pr_rf_build SELECT number FROM numbers(10);

-- Join runtime filters require the analyzer. On the old-analyzer runs the query plans with no join
-- and no parallel replicas at all, leaving the assertion below nothing to check.
SET enable_analyzer = 1;
SET enable_parallel_replicas = 1;
SET automatic_parallel_replicas_mode = 0;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET parallel_replicas_local_plan = 1;
SET parallel_replicas_min_number_of_rows_per_replica = 0;
SET enable_join_runtime_filters = 1;
-- The runtime filter has to be built whatever the probe side is estimated to be: under parallel
-- replicas that estimate is unknown, and the join order randomizer replaces it with a random one.
SET join_runtime_filter_min_probe_rows = 0;
SET parallel_replicas_filter_pushdown = 1;
-- Keep the small table on the build side so the plan shape below is stable.
SET query_plan_join_swap_table = false;
-- The plan below asserts that the filter reaches the read step as a `PREWHERE`, so pin the two
-- optimizations that fold it in.
SET query_plan_optimize_prewhere = 1;
SET optimize_move_to_prewhere = 1;
-- Pinned for a stable plan shape: the plan-based path sends a plan fragment rather than a query, so
-- the remote side reads as a single `ReadFromParallelReplicas` instead of `ReadFromRemoteParallelReplicas`.
SET parallel_replicas_plan_based = 0;

-- `description = 0` matters: with descriptions on, a plan-based parallel replicas read carries the
-- whole remote fragment as a multi-line `QueryPlan:` description, whose own `Aggregating` and
-- `ReadFromMergeTree` lines would satisfy the predicates below without saying anything about the local
-- plan. The indentation is kept for the same reason - it is what pins the runtime filter to the local
-- read rather than to some other step.
SELECT replaceAll(explain, currentDatabase(), 'default') AS step
FROM (
    EXPLAIN description = 0, actions = 1
    SELECT sum(agg.s)
    FROM (SELECT k, sum(v) AS s FROM pr_rf_probe GROUP BY k) AS agg
    JOIN pr_rf_build AS b ON agg.k = b.k
)
WHERE explain LIKE '%Aggregating%'
   OR explain LIKE '%ReadFrom%'
   OR explain LIKE '%Filter%'
   OR explain LIKE '%Runtime filters:%';

SELECT sum(agg.s)
FROM (SELECT k, sum(v) AS s FROM pr_rf_probe GROUP BY k) AS agg
JOIN pr_rf_build AS b ON agg.k = b.k;

DROP TABLE pr_rf_probe;
DROP TABLE pr_rf_build;
