-- Plain (non-replicated) MergeTree needs parallel_replicas_for_non_replicated_merge_tree to use parallel
-- replicas. A query over a plain table is gated in InterpreterSelectQuery, but a query through a VIEW
-- bypasses that gate (the outer storage is the view). Plan-based parallel replicas must still honour the
-- opt-in for the view's inner read

DROP TABLE IF EXISTS t_pr_nonrepl;
DROP VIEW IF EXISTS v_pr_nonrepl;

CREATE TABLE t_pr_nonrepl (a UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_pr_nonrepl SELECT number FROM numbers(1000);
CREATE VIEW v_pr_nonrepl AS SELECT a FROM t_pr_nonrepl;

SET enable_analyzer = 1;
SET enable_parallel_replicas = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_plan_based = 1;
SET automatic_parallel_replicas_mode = 0;

-- Opt-in OFF (default): the plain-MergeTree view must NOT be distributed. Correct result, no remote read.
SELECT sum(a) FROM v_pr_nonrepl SETTINGS parallel_replicas_for_non_replicated_merge_tree = 0;
SELECT countIf(explain LIKE '%ReadFromParallelReplicas%') > 0 AS has_remote_read
FROM (EXPLAIN optimize = 1, description = 0 SELECT sum(a) FROM v_pr_nonrepl SETTINGS parallel_replicas_for_non_replicated_merge_tree = 0);

-- Opt-in ON: the same view IS distributed. Correct result, remote read present.
SELECT sum(a) FROM v_pr_nonrepl SETTINGS parallel_replicas_for_non_replicated_merge_tree = 1;
SELECT countIf(explain LIKE '%ReadFromParallelReplicas%') > 0 AS has_remote_read
FROM (EXPLAIN optimize = 1, description = 0 SELECT sum(a) FROM v_pr_nonrepl SETTINGS parallel_replicas_for_non_replicated_merge_tree = 1);

DROP VIEW v_pr_nonrepl;
DROP TABLE t_pr_nonrepl;
