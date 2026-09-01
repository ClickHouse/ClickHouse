-- Regression test for plan-based parallel replicas over a branching fragment. With
-- parallel_replicas_allow_view_over_mergetree a view that expands to UNION ALL over MergeTree yields
-- a fragment with multiple sources. The fragment builder must clone the subtree structurally
-- (QueryPlan::cloneSubtree); the previous QueryPlan::addStep rebuild only handled a linear chain and
-- threw on the second leaf. Also requires UnionStep::clone. Results must match non-parallel execution.

DROP TABLE IF EXISTS t_pr_union_1;
DROP TABLE IF EXISTS t_pr_union_2;
DROP VIEW IF EXISTS v_pr_union;

CREATE TABLE t_pr_union_1 (a UInt64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t_pr_union_2 (a UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_pr_union_1 SELECT number FROM numbers(1000);
INSERT INTO t_pr_union_2 SELECT number + 1000 FROM numbers(1000);
CREATE VIEW v_pr_union AS SELECT a FROM t_pr_union_1 UNION ALL SELECT a FROM t_pr_union_2;

SET enable_analyzer = 1;
SET enable_parallel_replicas = 1;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_plan_based = 1;
SET parallel_replicas_local_plan = 1;
SET automatic_parallel_replicas_mode = 0;
SET parallel_replicas_allow_view_over_mergetree = 1;

-- Slow the initiator's local read so the remote replicas exercise the shipped branching fragment.
SYSTEM ENABLE FAILPOINT slowdown_parallel_replicas_local_plan_read;

SELECT count(), sum(a), min(a), max(a) FROM v_pr_union;

SYSTEM DISABLE FAILPOINT slowdown_parallel_replicas_local_plan_read;

-- Plan shape. Before optimization the marker sits above the view's UNION over both reads
-- (has_split, has_union, has_read; no remote read yet). After optimization the split is converted
-- into a UNION of a local read and a remote parallel-replicas read of the shipped fragment
-- (no split; union, local read and remote read present).
SELECT
    countIf(explain LIKE '%ParallelReplicasSplit%') > 0 AS has_split,
    countIf(explain LIKE '%Union%') > 0 AS has_union,
    countIf(explain LIKE '%ReadFromMergeTree%') > 0 AS has_read,
    countIf(explain LIKE '%ReadFromParallelReplicas%') > 0 AS has_remote_read
FROM (EXPLAIN optimize = 0, description = 0 SELECT count(), sum(a), min(a), max(a) FROM v_pr_union);

SELECT
    countIf(explain LIKE '%ParallelReplicasSplit%') > 0 AS has_split,
    countIf(explain LIKE '%Union%') > 0 AS has_union,
    countIf(explain LIKE '%ReadFromMergeTree%') > 0 AS has_read,
    countIf(explain LIKE '%ReadFromParallelReplicas%') > 0 AS has_remote_read
FROM (EXPLAIN optimize = 1, description = 0 SELECT count(), sum(a), min(a), max(a) FROM v_pr_union);

DROP VIEW v_pr_union;
DROP TABLE t_pr_union_1;
DROP TABLE t_pr_union_2;
