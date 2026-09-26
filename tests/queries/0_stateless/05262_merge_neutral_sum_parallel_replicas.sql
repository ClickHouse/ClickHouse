-- Tags: no-parallel-replicas
-- Explicitly configures the localhost replica fixture instead of runner randomization.
DROP TABLE IF EXISTS t05262_merge;
DROP TABLE IF EXISTS t05262_keys;
DROP TABLE IF EXISTS t05262_values;

SET max_threads=2;
-- Exercise rewrite semantics on small fixtures independently of the default cost gate.
SET optimize_merge_neutral_sum_children_min_read_bytes = 0;
SET log_queries=1;
SET log_queries_min_type='QUERY_FINISH';
CREATE TABLE t05262_values (k UInt64,pnl Nullable(Float64)) ENGINE=MergeTree ORDER BY k;
CREATE TABLE t05262_keys (k UInt64,d UInt64,PROJECTION p (SELECT k,sum(d) GROUP BY k)) ENGINE=MergeTree ORDER BY k;
INSERT INTO t05262_values VALUES (0,10),(0,20);
INSERT INTO t05262_keys SELECT number%3,1 FROM numbers(30000);
CREATE TABLE t05262_merge AS t05262_values ENGINE=Merge(currentDatabase(), '^t05262_(values|keys)$');
SET automatic_parallel_replicas_mode=0;
SET enable_parallel_replicas=1;
SET parallel_replicas_local_plan=0;
SET parallel_replicas_plan_based=1;
SET parallel_replicas_allow_merge_tables=1;
SET parallel_replicas_for_non_replicated_merge_tree=1;
SET max_parallel_replicas=3;
SET cluster_for_parallel_replicas='test_cluster_one_shard_three_replicas_localhost';
SELECT countIf(explain LIKE '%ReadFromParallelReplicas%') > 0
FROM (EXPLAIN description=0 SELECT k,sum(pnl) FROM t05262_merge GROUP BY k
SETTINGS optimize_merge_neutral_sum_children=1);
SELECT k,sum(pnl) FROM t05262_merge GROUP BY k ORDER BY k SETTINGS optimize_merge_neutral_sum_children=0;
SELECT k,sum(pnl) FROM t05262_merge GROUP BY k ORDER BY k SETTINGS optimize_merge_neutral_sum_children=1,log_comment='05262_on';
SYSTEM FLUSH LOGS;
SELECT empty(projections) FROM system.user_query_log
WHERE current_database=currentDatabase() AND type='QueryFinish' AND log_comment='05262_on' AND is_initial_query
ORDER BY event_time_microseconds DESC LIMIT 1;
SET enable_parallel_replicas=0;
DROP TABLE t05262_merge;
DROP TABLE t05262_keys;
DROP TABLE t05262_values;
