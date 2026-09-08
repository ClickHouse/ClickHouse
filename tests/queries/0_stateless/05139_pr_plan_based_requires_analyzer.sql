-- The plan-based implementation of parallel replicas requires the analyzer: without it the planner
-- never builds the distributed plan the optimization works on. Such a query must run locally, and
-- must NOT fall back to the query-based implementation, which the plan-based one is meant to
-- replace. `parallel_replicas_only_with_analyzer` is turned off below so that it is not the thing
-- doing the refusing.

SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS t_pr_needs_analyzer;

CREATE TABLE t_pr_needs_analyzer (a UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_pr_needs_analyzer SELECT number FROM numbers(1000);

SET enable_parallel_replicas = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_replicas_min_number_of_rows_per_replica = 0,
    automatic_parallel_replicas_mode = 0;

-- With the analyzer, the plan-based implementation ships a plan fragment.
SELECT 'analyzer: plan-based reads from replicas';
SELECT trimLeft(explain) FROM (EXPLAIN description = 0 SELECT sum(a) FROM t_pr_needs_analyzer)
WHERE explain LIKE '%ParallelReplicas%'
SETTINGS enable_analyzer = 1, parallel_replicas_plan_based = 1;

-- Without the analyzer, nothing reads from the replicas: the query runs locally. In particular this
-- must not be `ReadFromRemoteParallelReplicas`.
SELECT 'no analyzer: reads from replicas';
SELECT count() FROM (EXPLAIN description = 0 SELECT sum(a) FROM t_pr_needs_analyzer)
WHERE explain LIKE '%ParallelReplicas%'
SETTINGS enable_analyzer = 0, parallel_replicas_only_with_analyzer = 0, parallel_replicas_plan_based = 1;

-- The query-based implementation keeps working without the analyzer, as before.
SELECT 'no analyzer, query-based: reads from replicas';
SELECT trimLeft(explain) FROM (EXPLAIN description = 0 SELECT sum(a) FROM t_pr_needs_analyzer)
WHERE explain LIKE '%ParallelReplicas%'
SETTINGS enable_analyzer = 0, parallel_replicas_only_with_analyzer = 0, parallel_replicas_plan_based = 0;

-- Every variant returns the same result. `enable_analyzer` cannot be changed inside a subquery, so
-- these are separate statements and the reference holds the same sum three times.
SELECT 'results';
SELECT sum(a) FROM t_pr_needs_analyzer SETTINGS enable_analyzer = 1, parallel_replicas_plan_based = 1;
SELECT sum(a) FROM t_pr_needs_analyzer SETTINGS enable_analyzer = 0, parallel_replicas_only_with_analyzer = 0, parallel_replicas_plan_based = 1;
SELECT sum(a) FROM t_pr_needs_analyzer SETTINGS enable_analyzer = 0, parallel_replicas_only_with_analyzer = 0, parallel_replicas_plan_based = 0;

DROP TABLE t_pr_needs_analyzer;
