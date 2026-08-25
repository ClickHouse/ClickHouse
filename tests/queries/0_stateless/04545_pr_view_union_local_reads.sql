-- Regression test for the local fragment builder of plan-based parallel replicas over a view that
-- expands to UNION ALL. For a non-aggregating projection the split sits directly above the view's
-- UnionStep (the fragment root is the union). The local fragment builder must coordinate EVERY
-- ReadFromMergeTree leaf of that union, like the remote fragment does. Previously it relied on
-- findReadingSteps, which skips a root UnionStep, so only the first branch was coordinated locally;
-- the remaining branches were read fully on the initiator AND again from the remote fragment,
-- duplicating their rows. Results must match non-parallel execution.

DROP TABLE IF EXISTS t_pr_union_local_1;
DROP TABLE IF EXISTS t_pr_union_local_2;
DROP VIEW IF EXISTS v_pr_union_local;

CREATE TABLE t_pr_union_local_1 (a UInt64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t_pr_union_local_2 (a UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_pr_union_local_1 SELECT number FROM numbers(5);
INSERT INTO t_pr_union_local_2 SELECT number + 5 FROM numbers(5);
CREATE VIEW v_pr_union_local AS SELECT a FROM t_pr_union_local_1 UNION ALL SELECT a FROM t_pr_union_local_2;

SET enable_analyzer = 1;
SET enable_parallel_replicas = 1;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_plan_based = 1;
SET parallel_replicas_local_plan = 1;
SET automatic_parallel_replicas_mode = 0;
SET parallel_replicas_allow_view_over_mergetree = 1;

-- Slow the initiator's local read so the remote fragment actually produces rows; a second union
-- branch read both locally and remotely would then surface duplicated rows.
SYSTEM ENABLE FAILPOINT slowdown_parallel_replicas_local_plan_read;

-- Non-aggregating: the split stays directly above the view's UNION (Sorting does not absorb it), so
-- the fragment root is the union and every branch must be coordinated. ORDER BY gives a deterministic
-- order; each value must appear exactly once.
SELECT a FROM v_pr_union_local ORDER BY a;

SYSTEM DISABLE FAILPOINT slowdown_parallel_replicas_local_plan_read;

-- Plan shape. Before optimization the marker sits directly above the view's UNION over both reads
-- (has_split, has_union, has_read; no remote read yet). After optimization the split is converted into
-- a UNION of a local read and a remote parallel-replicas read of the shipped fragment (no split; union,
-- local read and remote read present).
SELECT
    countIf(explain LIKE '%ParallelReplicasSplit%') > 0 AS has_split,
    countIf(explain LIKE '%Union%') > 0 AS has_union,
    countIf(explain LIKE '%ReadFromMergeTree%') > 0 AS has_read,
    countIf(explain LIKE '%ReadFromParallelReplicas%') > 0 AS has_remote_read
FROM (EXPLAIN optimize = 0, description = 0 SELECT a FROM v_pr_union_local ORDER BY a);

SELECT
    countIf(explain LIKE '%ParallelReplicasSplit%') > 0 AS has_split,
    countIf(explain LIKE '%Union%') > 0 AS has_union,
    countIf(explain LIKE '%ReadFromMergeTree%') > 0 AS has_read,
    countIf(explain LIKE '%ReadFromParallelReplicas%') > 0 AS has_remote_read
FROM (EXPLAIN optimize = 1, description = 0 SELECT a FROM v_pr_union_local ORDER BY a);

DROP VIEW v_pr_union_local;
DROP TABLE t_pr_union_local_1;
DROP TABLE t_pr_union_local_2;
