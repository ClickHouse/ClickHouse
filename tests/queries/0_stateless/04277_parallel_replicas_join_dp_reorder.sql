-- Regression test: the DP join-order optimizer must keep running when parallel replicas is enabled.
-- Previously `QueryPlanOptimizationSettings` forced `query_plan_optimize_join_order_limit=0`
-- whenever `allow_experimental_parallel_reading_from_replicas && max_parallel_replicas > 1`, which
-- silently skipped join reordering for any PR query.

DROP TABLE IF EXISTS pr_dp_small;
DROP TABLE IF EXISTS pr_dp_large;

CREATE TABLE pr_dp_small (x UInt32) ENGINE = MergeTree ORDER BY x AS SELECT number FROM numbers(100);
CREATE TABLE pr_dp_large (x UInt32) ENGINE = MergeTree ORDER BY x AS SELECT number FROM numbers(100000);

-- `small JOIN large` is the shape DP swaps so the larger table ends up on the left (probe) side.
-- Extract the table names of `ReadFromMergeTree` steps from the plan in tree-walk order — left
-- child first. With DP active, the larger table should appear before the smaller one; with DP
-- disabled (the previous forced behaviour), the user-written order would be preserved.
SELECT extract(explain, '(pr_dp_small|pr_dp_large)')
FROM ( EXPLAIN PLAN
    SELECT count() FROM pr_dp_small INNER JOIN pr_dp_large USING (x)
    SETTINGS
        enable_parallel_replicas = 1,
        cluster_for_parallel_replicas = 'parallel_replicas',
        max_parallel_replicas = 3,
        parallel_replicas_for_non_replicated_merge_tree = 1,
        parallel_replicas_local_plan = 1,
        query_plan_join_swap_table = 'auto',
        query_plan_optimize_join_order_limit = 10
)
WHERE explain LIKE '%ReadFromMergeTree%';

DROP TABLE pr_dp_small;
DROP TABLE pr_dp_large;
