-- With plan-based parallel replicas, a query over a view whose underlying query is a UNION ALL over
-- MergeTree is parallelized WITHOUT enabling parallel_replicas_allow_view_over_mergetree. At plan level
-- the view is expanded into a UnionStep over plain reads, and the applyParallelReplicas plan
-- optimization distributes them (that flag is a query-tree-level concern the plan-based path does not
-- need). Results must match non-parallel execution.

DROP TABLE IF EXISTS t_pr_novf_1;
DROP TABLE IF EXISTS t_pr_novf_2;
DROP VIEW IF EXISTS v_pr_novf;

CREATE TABLE t_pr_novf_1 (a UInt64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t_pr_novf_2 (a UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_pr_novf_1 SELECT number FROM numbers(1000);
INSERT INTO t_pr_novf_2 SELECT number + 1000 FROM numbers(1000);
CREATE VIEW v_pr_novf AS SELECT a FROM t_pr_novf_1 UNION ALL SELECT a FROM t_pr_novf_2;

SET enable_analyzer = 1;
SET enable_parallel_replicas = 1;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_plan_based = 1;
SET parallel_replicas_local_plan = 1;
SET automatic_parallel_replicas_mode = 0;
-- parallel_replicas_allow_view_over_mergetree is left at its default (0): not needed for plan-based.

SELECT count(), sum(a), min(a), max(a) FROM v_pr_novf;

-- Plan shape. optimize=0 (planner) is a plain local plan: the view's UNION over plain reads, with no
-- split marker and no remote read (has_union, has_read). optimize=1: the split analysis distributes it
-- into a UNION of a local read and a remote parallel-replicas read of the shipped fragment (no split;
-- union, local read and remote read present) -- all without allow_view_over_mergetree.
SELECT
    countIf(explain LIKE '%ParallelReplicasSplit%') > 0 AS has_split,
    countIf(explain LIKE '%Union%') > 0 AS has_union,
    countIf(explain LIKE '%ReadFromMergeTree%') > 0 AS has_read,
    countIf(explain LIKE '%ReadFromParallelReplicas%') > 0 AS has_remote_read
FROM (EXPLAIN optimize = 0, description = 0 SELECT count(), sum(a) FROM v_pr_novf);

SELECT
    countIf(explain LIKE '%ParallelReplicasSplit%') > 0 AS has_split,
    countIf(explain LIKE '%Union%') > 0 AS has_union,
    countIf(explain LIKE '%ReadFromMergeTree%') > 0 AS has_read,
    countIf(explain LIKE '%ReadFromParallelReplicas%') > 0 AS has_remote_read
FROM (EXPLAIN optimize = 1, description = 0 SELECT count(), sum(a) FROM v_pr_novf);

DROP VIEW v_pr_novf;
DROP TABLE t_pr_novf_1;
DROP TABLE t_pr_novf_2;
