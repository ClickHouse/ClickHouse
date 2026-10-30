-- Tags: no-fasttest
-- no-fasttest: the distributed plan and parallel replicas need the stateless worker cluster configuration.
-- The per-block streaming flush of `group_by_each_block_no_merge` cannot produce the bucket-ordered partial
-- aggregation that the memory-efficient distributed merge consumes. The planners reject the combination for
-- ordinary distributed queries (see 04322); this test covers the post-planning rewrites that clone an
-- `AggregatingStep` into a partial stage and force bucket order: the distributed plan (`make_distributed_plan`,
-- rule-based and Cascades) and plan-based parallel replicas. In all of them the aggregation must stay correct:
-- no row is lost, so the per-block counts and sums add up to the totals.

DROP TABLE IF EXISTS t_gb_no_merge_dist_plan;

CREATE TABLE t_gb_no_merge_dist_plan (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_gb_no_merge_dist_plan SELECT number % 300, number FROM numbers(10000);
INSERT INTO t_gb_no_merge_dist_plan SELECT number % 300, number FROM numbers(10000, 10000);

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET max_rows_to_group_by = 0;
SET distributed_plan_default_shuffle_join_bucket_count = 3, distributed_plan_default_reader_bucket_count = 3;

SELECT 'make_distributed_plan, falls back to local execution';
SELECT sum(c), sum(s) FROM
(
    SELECT k, count() AS c, sum(v) AS s FROM t_gb_no_merge_dist_plan GROUP BY k
    SETTINGS group_by_each_block_no_merge = 1, max_block_size = 1000
)
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_aggregation_memory_efficient = 1;

SELECT 'make_distributed_plan, fallback disabled';
SELECT sum(c), sum(s) FROM
(
    SELECT k, count() AS c, sum(v) AS s FROM t_gb_no_merge_dist_plan GROUP BY k
    SETTINGS group_by_each_block_no_merge = 1, max_block_size = 1000
)
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_aggregation_memory_efficient = 1,
    distributed_plan_fallback_to_local_execution = 0; -- { serverError SUPPORT_IS_DISABLED }

SELECT 'make_distributed_plan, Cascades, falls back to local execution';
SELECT sum(c), sum(s) FROM
(
    SELECT k, count() AS c, sum(v) AS s FROM t_gb_no_merge_dist_plan GROUP BY k
    SETTINGS group_by_each_block_no_merge = 1, max_block_size = 1000
)
SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1, distributed_plan_execute_locally = 1,
    distributed_aggregation_memory_efficient = 1;

SELECT 'make_distributed_plan, Cascades, fallback disabled';
SELECT sum(c), sum(s) FROM
(
    SELECT k, count() AS c, sum(v) AS s FROM t_gb_no_merge_dist_plan GROUP BY k
    SETTINGS group_by_each_block_no_merge = 1, max_block_size = 1000
)
SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1, distributed_plan_execute_locally = 1,
    distributed_aggregation_memory_efficient = 1, distributed_plan_fallback_to_local_execution = 0; -- { serverError SUPPORT_IS_DISABLED }

-- Without the memory-efficient merge the partial stage does not have to be bucket-ordered, so the
-- distributed plan is built: the per-block states are gathered and merged on the initiator.
SELECT 'make_distributed_plan, without the memory-efficient merge';
SELECT sum(c), sum(s) FROM
(
    SELECT k, count() AS c, sum(v) AS s FROM t_gb_no_merge_dist_plan GROUP BY k
    SETTINGS group_by_each_block_no_merge = 1, max_block_size = 1000
)
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_aggregation_memory_efficient = 0,
    distributed_plan_fallback_to_local_execution = 0;

SET enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1;

-- Plan-based parallel replicas: with the memory-efficient merge the rewrite would need a bucket-ordered
-- partial stage on the replicas, so the aggregation stays on the initiator and the replicas return the
-- rows they read.
SELECT 'plan-based parallel replicas';
SELECT sum(c), sum(s) FROM
(
    SELECT k, count() AS c, sum(v) AS s FROM t_gb_no_merge_dist_plan GROUP BY k
    SETTINGS group_by_each_block_no_merge = 1, max_block_size = 1000
)
SETTINGS parallel_replicas_plan_based = 1, distributed_aggregation_memory_efficient = 1;

-- Without it the replicas run the per-block partial aggregation and the initiator merges the states.
SELECT 'plan-based parallel replicas, without the memory-efficient merge';
SELECT sum(c), sum(s) FROM
(
    SELECT k, count() AS c, sum(v) AS s FROM t_gb_no_merge_dist_plan GROUP BY k
    SETTINGS group_by_each_block_no_merge = 1, max_block_size = 1000
)
SETTINGS parallel_replicas_plan_based = 1, distributed_aggregation_memory_efficient = 0;

-- The query-tree-based parallel replicas plan the remote aggregation like a distributed query, so the
-- planner rejects the bucket-ordered combination up front (see 04322).
SELECT 'query-tree-based parallel replicas';
SELECT sum(c), sum(s) FROM
(
    SELECT k, count() AS c, sum(v) AS s FROM t_gb_no_merge_dist_plan GROUP BY k
    SETTINGS group_by_each_block_no_merge = 1, max_block_size = 1000
)
SETTINGS parallel_replicas_plan_based = 0, distributed_aggregation_memory_efficient = 1; -- { serverError NOT_IMPLEMENTED }

SELECT sum(c), sum(s) FROM
(
    SELECT k, count() AS c, sum(v) AS s FROM t_gb_no_merge_dist_plan GROUP BY k
    SETTINGS group_by_each_block_no_merge = 1, max_block_size = 1000
)
SETTINGS parallel_replicas_plan_based = 0, distributed_aggregation_memory_efficient = 0,
    enable_memory_bound_merging_of_aggregation_results = 0;

DROP TABLE t_gb_no_merge_dist_plan;
