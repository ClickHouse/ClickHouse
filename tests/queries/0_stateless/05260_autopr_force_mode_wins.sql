-- Tags: no-old-analyzer
-- no-old-analyzer: `automatic_parallel_replicas_mode` is implemented only in the analyzer.

-- Forcing parallel replicas (`enable_parallel_replicas = 2`) turns `automatic_parallel_replicas_mode` off, so a
-- `MergeTree` read uses parallel replicas whatever the automatic mode is. With `enable_parallel_replicas = 1` the
-- automatic mode decides, and it keeps a read this small local.

CREATE TABLE t (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t SELECT number FROM numbers(1000);

SET max_parallel_replicas = 3, parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_local_plan = 1,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';

SELECT sum(k) FROM t FORMAT Null SETTINGS enable_parallel_replicas = 2, automatic_parallel_replicas_mode = 0, log_comment = '05260_force_mode_0';
SELECT sum(k) FROM t FORMAT Null SETTINGS enable_parallel_replicas = 2, automatic_parallel_replicas_mode = 1, log_comment = '05260_force_mode_1';
SELECT sum(k) FROM t FORMAT Null SETTINGS enable_parallel_replicas = 2, automatic_parallel_replicas_mode = 2, log_comment = '05260_force_mode_2';
SELECT sum(k) FROM t FORMAT Null SETTINGS enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 1, log_comment = '05260_enabled_mode_1';

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['ParallelReplicasUsedCount'] > 0
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('05260_force_mode_0', '05260_force_mode_1', '05260_force_mode_2', '05260_enabled_mode_1')
    AND initial_query_id = query_id
ORDER BY log_comment
SETTINGS enable_parallel_replicas = 0;
