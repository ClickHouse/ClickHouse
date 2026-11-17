-- Tags: no-parallel-replicas
-- Regression test for plan-based parallel replicas over a UNION ALL whose branches read the SAME table.
-- Such a union must not be combined into one distributed fragment: the parallel-replicas coordinator
-- drives every read of a fragment and cannot distinguish duplicate announcements for one table (mirrors
-- StorageView's duplicate-table guard). applyParallelReplicas leaves it local instead. Results must match
-- non-parallel execution (UNION ALL counts the rows twice). See PR #111063 review.

DROP TABLE IF EXISTS t_pr_stu;
DROP VIEW IF EXISTS v_pr_stu;

CREATE TABLE t_pr_stu (a UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_pr_stu SELECT number FROM numbers(1000);
CREATE VIEW v_pr_stu AS SELECT a FROM t_pr_stu UNION ALL SELECT a FROM t_pr_stu;

SET enable_analyzer = 1;
SET enable_parallel_replicas = 1;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_plan_based = 1;
SET parallel_replicas_local_plan = 1;
SET automatic_parallel_replicas_mode = 0;

-- Correctness: same-table UNION ALL counts rows twice; must match non-parallel and not throw an exception. Both the
-- top-level union and a view expanding to the same union.
SELECT count(), sum(a) FROM (SELECT a FROM t_pr_stu UNION ALL SELECT a FROM t_pr_stu);
SELECT count(), sum(a) FROM v_pr_stu;

-- Plan shape: the same-table union is NOT shipped as a distributed fragment (no remote read).
SELECT countIf(explain LIKE '%ReadFromParallelReplicas%') > 0 AS has_remote_read
FROM (EXPLAIN optimize = 1, description = 0 SELECT a FROM t_pr_stu UNION ALL SELECT a FROM t_pr_stu);

DROP VIEW v_pr_stu;
DROP TABLE t_pr_stu;
