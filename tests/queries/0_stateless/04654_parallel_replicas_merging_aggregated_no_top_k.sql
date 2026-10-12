-- The plan-based parallel-replicas rewrite (`applyParallelReplicas`) copies the
-- partial aggregation's params into the initiator's `MergingAggregatedStep`.
-- The top-K heap belongs to the replicas' partial aggregation only: the merge
-- path never runs it, so the merge step must not carry `top_k` (it would skip
-- the hash-table size hints and advertise a Top-K in EXPLAIN that never runs).

-- With serialized plans the pass returns before it examines the merge step;
-- pin the text-planned path so the assertion tests the right gate.
SET serialize_query_plan = 0;

SET max_rows_to_group_by = 0;
SET optimize_trivial_group_by_limit_query = 0;
-- CI randomizes query_plan_max_limit_for_top_k_optimization (can be tiny); pin it.
SET query_plan_max_limit_for_top_k_optimization = 1000;
SET enable_analyzer = 1;
SET enable_group_by_top_k_optimization = 1;
-- Memory-bound merging and in-order aggregation turn the partial aggregation
-- into an in-order one (`applyOrder`), which by design drops the top-K heap;
-- pin both off so the partial steps keep the heap this test observes.
SET enable_memory_bound_merging_of_aggregation_results = 0;
SET optimize_aggregation_in_order = 0;

SET enable_parallel_replicas = 1;
SET parallel_replicas_plan_based = 1;
-- CI randomizes the automatic mode, whose cost model can silently skip the
-- parallel-replicas rewrite for a table this small; pin the explicit mode.
SET automatic_parallel_replicas_mode = 0;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'parallel_replicas';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET parallel_replicas_local_plan = 1;
SET parallel_replicas_prefer_local_replica = 1;

DROP TABLE IF EXISTS t_pr_merge_top_k;

CREATE TABLE t_pr_merge_top_k (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;

INSERT INTO t_pr_merge_top_k SELECT number % 1000, number FROM numbers(100000);

-- The late top-K optimization pass sees only the initiator's merge step after
-- the parallel-replicas rewrite. It must not annotate that merge step, because
-- the top-K heap runs only during partial aggregation on the replicas.
SELECT replaceRegexpOne(explain, '^[│└├─ ]+', '') FROM
(
    EXPLAIN actions = 1
    SELECT k, count() FROM t_pr_merge_top_k GROUP BY k ORDER BY k LIMIT 5
)
WHERE explain LIKE '%MergingAggregated%' OR explain LIKE '%Top-K%';

SELECT k, count() FROM t_pr_merge_top_k GROUP BY k ORDER BY k LIMIT 5;

DROP TABLE t_pr_merge_top_k;
