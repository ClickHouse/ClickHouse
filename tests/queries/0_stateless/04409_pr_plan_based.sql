-- Tests parallel_replicas_exchange_plan: the planner builds a plain local plan, then a post-build phase
-- splits it at the reading step into a UNION of a local read and a remote parallel-replicas read of the
-- shipped fragment. Results must match non-parallel execution, and counts must not be multiplied across
-- replicas (regression guard: each replica reads disjoint ranges, coordinated via the shared coordinator).

DROP TABLE IF EXISTS t_pr_plan_based;

CREATE TABLE t_pr_plan_based (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_pr_plan_based SELECT number, number % 10 FROM numbers(100000);

SET enable_parallel_replicas = 1;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_plan_based = 1;

-- Correctness: identical to non-parallel execution. count() is the key regression guard against the
-- "each replica reads everything" (N x) bug.
SELECT count(), sum(b), min(a), max(a) FROM t_pr_plan_based WHERE a > 5;
SELECT b, count() FROM t_pr_plan_based GROUP BY b ORDER BY b;
SELECT count() FROM t_pr_plan_based;

-- The read is split into a local read + a remote parallel-replicas read (deterministic, no addresses).
SELECT
    countIf(explain LIKE '%ParallelReplicasSplitStep%') > 0 AS has_split,
    countIf(explain LIKE '%Union%') > 0 AS has_union,
    countIf(explain LIKE '%ReadFromRemoteParallelReplicas%') > 0 AS has_remote_read,
    countIf(explain LIKE '%ReadFromMergeTree%') > 0 AS has_local_read
FROM (EXPLAIN SELECT sum(b) FROM t_pr_plan_based WHERE a > 5);

-- DROP TABLE t_pr_plan_based;
