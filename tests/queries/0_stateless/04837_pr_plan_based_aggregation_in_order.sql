-- Tags: no-random-merge-tree-settings

-- Aggregation in order under plan-based parallel replicas (`parallel_replicas_plan_based`).
--
-- The split is lifted through the `AggregatingStep` the same way it is lifted through the `SortingStep` (see
-- 04836_pr_plan_based_read_in_order): a partial aggregation ships inside the fragment and the initiator keeps a
-- `MergingAggregated` above the union. The worker re-optimizes the deserialized fragment with its own settings,
-- so `optimizeAggregationInOrder` can fire there and request an in-order read; the initiator's local half is
-- re-optimized on a separate path. Both sides must independently derive the same `CoordinationMode`, otherwise
-- the coordinator throws "Replica ... decided to read in ... mode, not in ...". This is the same failure shape
-- as 03810_pr_aggr_in_order_read_mode and 04009_pr_aggr_in_order_coordination_mode cover for classic parallel
-- replicas.

DROP TABLE IF EXISTS t_pr_aggr_in_order;

CREATE TABLE t_pr_aggr_in_order (a UInt64, b UInt64)
ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 128;

-- `a` is a primary-key prefix with many rows per key, so aggregation in order applies.
INSERT INTO t_pr_aggr_in_order SELECT number % 1000, number FROM numbers(100000);
OPTIMIZE TABLE t_pr_aggr_in_order FINAL;

SET enable_analyzer = 1;
SET enable_parallel_replicas = 1;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET optimize_aggregation_in_order = 1;
SET parallel_replicas_plan_based = 1;
-- Pin the manual mode: CI randomizes `automatic_parallel_replicas_mode` to 2, and the cost model may then
-- decide against parallel replicas, so the plan-based split would never engage.
SET automatic_parallel_replicas_mode = 0;

-- Aggregation on the primary-key prefix must match non-parallel execution exactly. Aggregating the
-- per-group results keeps the reference small while still failing if the partial aggregation shipped with the
-- fragment and the `MergingAggregated` above the union do not add up to the same groups.
SELECT '--- GROUP BY pk, local ---';
SELECT count(), sum(cnt), sum(s) FROM
    (SELECT a, count() AS cnt, sum(b) AS s FROM t_pr_aggr_in_order GROUP BY a)
SETTINGS enable_parallel_replicas = 0;
SELECT '--- GROUP BY pk, plan_based = 1, local_plan = 1 ---';
SELECT count(), sum(cnt), sum(s) FROM
    (SELECT a, count() AS cnt, sum(b) AS s FROM t_pr_aggr_in_order GROUP BY a)
SETTINGS parallel_replicas_local_plan = 1;
SELECT '--- GROUP BY pk, plan_based = 1, local_plan = 0 ---';
SELECT count(), sum(cnt), sum(s) FROM
    (SELECT a, count() AS cnt, sum(b) AS s FROM t_pr_aggr_in_order GROUP BY a)
SETTINGS parallel_replicas_local_plan = 0;

-- The first groups in key order, so a wrong in-order merge shows up as reordered or merged groups rather
-- than only as a bad total.
SELECT '--- first groups in key order, local ---';
SELECT a, count() AS cnt FROM t_pr_aggr_in_order GROUP BY a ORDER BY a LIMIT 5
SETTINGS enable_parallel_replicas = 0;
SELECT '--- first groups in key order, plan_based = 1 ---';
SELECT a, count() AS cnt FROM t_pr_aggr_in_order GROUP BY a ORDER BY a LIMIT 5;

-- The 04009 shape: force every replica to announce, including ones that end up with no parts. That is what
-- makes an initiator/worker coordination-mode disagreement deterministic rather than racy - the initiator
-- with 0 parts takes a separate announcement path that derives the mode from `input_order_info`.
-- The failpoint is ONCE, so it has to be re-armed before each query.
SELECT '--- all replicas announce (empty result expected), plan_based = 1, local_plan = 1 ---';
SYSTEM ENABLE FAILPOINT parallel_replicas_wait_for_unused_replicas;
SELECT a FROM t_pr_aggr_in_order GROUP BY a HAVING materialize(0)
SETTINGS parallel_replicas_local_plan = 1;

SELECT '--- all replicas announce (empty result expected), plan_based = 1, local_plan = 0 ---';
SYSTEM ENABLE FAILPOINT parallel_replicas_wait_for_unused_replicas;
SELECT a FROM t_pr_aggr_in_order GROUP BY a HAVING materialize(0)
SETTINGS parallel_replicas_local_plan = 0;

SYSTEM DISABLE FAILPOINT parallel_replicas_wait_for_unused_replicas;

-- Both EXPLAIN checks below describe the initiator's own half of the read, so the local plan has to exist.
-- CI randomizes `parallel_replicas_local_plan`, and with 0 there is no local read at all: no UNION and no
-- in-order pool on the initiator.
SET parallel_replicas_local_plan = 1;

-- The split engaged, and the aggregation was split into a partial aggregation (shipped inside the fragment)
-- and a `MergingAggregated` above the union on the initiator.
SELECT '--- explain: has_union, has_remote_read, has_merging_aggregated ---';
SELECT
    countIf(explain LIKE '%Union%') > 0 AS has_union,
    countIf(explain LIKE '%ReadFromParallelReplicas%') > 0 AS has_remote_read,
    countIf(explain LIKE '%MergingAggregated%') > 0 AS has_merging_aggregated
FROM (EXPLAIN pretty = 0, description = 0 SELECT a, count() FROM t_pr_aggr_in_order GROUP BY a);

-- Both sides derive an in-order read from the shipped partial aggregation, so the coordinator runs in
-- `WithOrder` mode. This catches a silent degradation to `Default`, and equally a regression where only one
-- side degrades and the coordinator throws.
SELECT '--- explain pipeline: in_order_pool, aggregating_in_order ---';
SELECT
    countIf(explain LIKE '%ReadPoolParallelReplicasInOrder%') > 0 AS in_order_pool,
    countIf(explain LIKE '%AggregatingInOrder%') > 0 AS aggregating_in_order
FROM (EXPLAIN PIPELINE SELECT a, count() FROM t_pr_aggr_in_order GROUP BY a);

DROP TABLE t_pr_aggr_in_order;
