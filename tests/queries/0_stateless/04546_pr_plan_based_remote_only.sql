-- Regression test: plan-based parallel replicas with parallel_replicas_prefer_local_replica = 0 must
-- still distribute the read via a remote-only fragment over all replicas, not silently fall back to a
-- single-node local read. Previously createParallelReplicasPlan handled only the local-plan branch and
-- returned nullptr otherwise, leaving the ParallelReplicasSplitStep as a pass-through. Results must
-- match non-parallel execution.

DROP TABLE IF EXISTS t_pr_remote_only;

CREATE TABLE t_pr_remote_only (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_pr_remote_only SELECT number, number % 10 FROM numbers(100000);

SET enable_analyzer = 1;
SET enable_parallel_replicas = 1;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_plan_based = 1;
SET automatic_parallel_replicas_mode = 0;
-- No local plan: exercise the remote-only branch of createParallelReplicasPlan.
SET parallel_replicas_prefer_local_replica = 0;

SELECT count(), sum(b), min(a), max(a) FROM t_pr_remote_only WHERE a > 5;

-- Plan shape. Before optimization the split marker sits above the read (has_split, has_read). After
-- optimization it is replaced by a remote parallel-replicas read of the shipped fragment, with no local
-- read and no local/remote union (remote-only, since prefer_local_replica = 0).
SELECT
    countIf(explain LIKE '%ParallelReplicasSplit%') > 0 AS has_split,
    countIf(explain LIKE '%Union%') > 0 AS has_union,
    countIf(explain LIKE '%ReadFromMergeTree%') > 0 AS has_read,
    countIf(explain LIKE '%ReadFromParallelReplicas%') > 0 AS has_remote_read
FROM (EXPLAIN optimize = 0, description = 0 SELECT sum(b) FROM t_pr_remote_only WHERE a > 5);

SELECT
    countIf(explain LIKE '%ParallelReplicasSplit%') > 0 AS has_split,
    countIf(explain LIKE '%Union%') > 0 AS has_union,
    countIf(explain LIKE '%ReadFromMergeTree%') > 0 AS has_read,
    countIf(explain LIKE '%ReadFromParallelReplicas%') > 0 AS has_remote_read
FROM (EXPLAIN optimize = 1, description = 0 SELECT sum(b) FROM t_pr_remote_only WHERE a > 5);

DROP TABLE t_pr_remote_only;
