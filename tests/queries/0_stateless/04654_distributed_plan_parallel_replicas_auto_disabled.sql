-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.

-- make_distributed_plan used to throw SUPPORT_IS_DISABLED when enable_parallel_replicas (or the
-- automatic parallel replicas heuristic) was on. It now auto-disables parallel replicas instead
-- (issue #109476).

DROP TABLE IF EXISTS t_dp_pr;
CREATE TABLE t_dp_pr (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_dp_pr SELECT number, number FROM numbers(100000);

SET make_distributed_plan = 1, distributed_plan_execute_locally = 1,
    distributed_plan_max_rows_to_broadcast = 0, distributed_plan_default_reader_bucket_count = 3,
    distributed_plan_default_shuffle_join_bucket_count = 3, max_rows_to_group_by = 0;

SELECT 'enable_parallel_replicas = 1 no longer throws';
SELECT count(), sum(v) FROM t_dp_pr
    SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3;

SELECT 'the automatic parallel replicas heuristic no longer throws';
SELECT count(), sum(v) FROM t_dp_pr
    SETTINGS enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 2, parallel_replicas_local_plan = 1;

-- A per-query SETTINGS clause is re-applied onto the query's own context during query tree
-- building, so the auto-switch must hold for it too. Without that, this combination planned a
-- parallel-replicas local read and failed on `QueryPlanOptimizationSettings` construction.
SELECT 'parallel replicas over a plain MergeTree with a cluster no longer throws';
SELECT count(), sum(v) FROM t_dp_pr
    SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3,
        cluster_for_parallel_replicas = 'parallel_replicas',
        parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_local_plan = 1;

SELECT 'the query still distributes';
-- sum() and not a bare count() so the trivial-count optimization cannot fold the plan away.
SELECT 'distributes'
FROM (EXPLAIN PIPELINE SELECT sum(v) FROM t_dp_pr SETTINGS enable_parallel_replicas = 1)
WHERE explain LIKE '%ReadFromDistributedPlanSource%' LIMIT 1;

DROP TABLE t_dp_pr;
