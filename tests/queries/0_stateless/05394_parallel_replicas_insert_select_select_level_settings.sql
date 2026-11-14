-- The SELECT's own SETTINGS can rule out the local plan that the INSERT-level settings allow (issue #122729).
SET enable_analyzer = 1; -- parallel distributed insert select for replicated tables works only with analyzer
SET parallel_distributed_insert_select = 2;

DROP TABLE IF EXISTS t_mt_source;
DROP TABLE IF EXISTS t_rmt_target SYNC;

CREATE TABLE t_mt_source (k UInt64, v String) ENGINE = MergeTree() ORDER BY k;
CREATE TABLE t_rmt_target (k UInt64, v String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_rmt_target', 'r1') ORDER BY ();

INSERT INTO t_mt_source SELECT number AS k, toString(number) AS v FROM system.numbers LIMIT 1e5;

SET enable_parallel_replicas = 1, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', max_parallel_replicas = 3;
SET parallel_replicas_for_non_replicated_merge_tree = 1;

SELECT '-- parallel_replicas_local_plan = 0 in the SELECT';
TRUNCATE TABLE t_rmt_target;
INSERT INTO t_rmt_target SETTINGS parallel_replicas_local_plan = 1, log_comment = '05394_local_plan'
SELECT * FROM t_mt_source SETTINGS parallel_replicas_local_plan = 0;
SELECT count() FROM t_rmt_target;
SELECT * FROM t_rmt_target EXCEPT SELECT * FROM t_mt_source;

SELECT '-- parallel_replicas_prefer_local_replica = 0 in the SELECT';
TRUNCATE TABLE t_rmt_target;
INSERT INTO t_rmt_target SETTINGS parallel_replicas_local_plan = 1, parallel_replicas_prefer_local_replica = 1, log_comment = '05394_prefer_local'
SELECT * FROM t_mt_source SETTINGS parallel_replicas_prefer_local_replica = 0;
SELECT count() FROM t_rmt_target;
SELECT * FROM t_rmt_target EXCEPT SELECT * FROM t_mt_source;

SELECT '-- max_execution_time_leaf = 0 in the INSERT, 100 in the SELECT';
TRUNCATE TABLE t_rmt_target;
INSERT INTO t_rmt_target SETTINGS parallel_replicas_local_plan = 1, max_execution_time = 300, max_execution_time_leaf = 0, log_comment = '05394_leaf_timeout'
SELECT * FROM t_mt_source SETTINGS max_execution_time_leaf = 100;
SELECT count() FROM t_rmt_target;
SELECT * FROM t_rmt_target EXCEPT SELECT * FROM t_mt_source;

SELECT '-- agreeing settings use the local pipeline';
TRUNCATE TABLE t_rmt_target;
INSERT INTO t_rmt_target SETTINGS parallel_replicas_local_plan = 1, log_comment = '05394_control'
SELECT * FROM t_mt_source;
SELECT count() FROM t_rmt_target;
SELECT * FROM t_rmt_target EXCEPT SELECT * FROM t_mt_source;

-- Executed INSERTs: the initiator plus one per remote replica, so 4 without the local pipeline and 3 with it.
SYSTEM FLUSH LOGS query_log;
SELECT log_comment, count() FROM system.query_log
WHERE (current_database = currentDatabase() OR has(databases, currentDatabase())) AND type = 'QueryFinish' AND query_kind = 'Insert'
    AND log_comment IN ('05394_local_plan', '05394_prefer_local', '05394_leaf_timeout', '05394_control') AND event_date >= yesterday() AND event_time >= now() - 600
GROUP BY log_comment ORDER BY log_comment;

DROP TABLE t_mt_source;
DROP TABLE t_rmt_target SYNC;
