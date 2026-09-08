-- Plan-based parallel replicas is not supported without the analyzer, so it must not be applied
-- there. `canUseParallelReplicasOnInitiator` already refuses when the analyzer is off, but only
-- while `parallel_replicas_only_with_analyzer` is set - turning that off must fall back to the
-- query-based implementation rather than reach the plan-based one.

SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS t_pr_needs_analyzer;

CREATE TABLE t_pr_needs_analyzer (a UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_pr_needs_analyzer SELECT number FROM numbers(1000);

SET enable_parallel_replicas = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_replicas_min_number_of_rows_per_replica = 0,
    automatic_parallel_replicas_mode = 0,
    parallel_replicas_plan_based = 1;

-- With the analyzer the plan-based implementation ships a plan fragment: `ReadFromParallelReplicas`.
SELECT 'analyzer, plan-based';
SELECT trimLeft(explain) FROM (EXPLAIN description = 0 SELECT sum(a) FROM t_pr_needs_analyzer)
WHERE explain LIKE '%ParallelReplicas%' SETTINGS enable_analyzer = 1;

-- Without the analyzer it must be the query-based `ReadFromRemoteParallelReplicas`, even though
-- `parallel_replicas_plan_based` is on and `parallel_replicas_only_with_analyzer` is off.
SELECT 'no analyzer, falls back to query-based';
SELECT trimLeft(explain) FROM (EXPLAIN description = 0 SELECT sum(a) FROM t_pr_needs_analyzer)
WHERE explain LIKE '%ParallelReplicas%'
SETTINGS enable_analyzer = 0, parallel_replicas_only_with_analyzer = 0;

-- Results are the same either way. `enable_analyzer` cannot be changed inside a subquery, so the two
-- reads are separate statements and the reference holds the same sum twice.
SELECT 'results';
SELECT sum(a) FROM t_pr_needs_analyzer SETTINGS enable_analyzer = 1;
SELECT sum(a) FROM t_pr_needs_analyzer SETTINGS enable_analyzer = 0, parallel_replicas_only_with_analyzer = 0;

DROP TABLE t_pr_needs_analyzer;
