-- When PREWHERE takes over a filter, the single-node plan keeps a rename-only `Expression` that the
-- parallel replicas local branch merges away. The plan hash ignores such steps, so the two plans match,
-- and the walk that pairs their reads has to ignore them too - otherwise it gives up and no statistics
-- are collected. Under `parallel_replicas_plan_based = 1`, which CI does not randomize.

DROP TABLE IF EXISTS t_pb_rename;

CREATE TABLE t_pb_rename (key UInt64, value UInt64) ENGINE = MergeTree ORDER BY key
    SETTINGS index_granularity = 8192;

INSERT INTO t_pb_rename SELECT number, number FROM numbers(200000);

SET optimize_move_to_prewhere = 1;
-- An aggregation that spills marks the statistics as unsupported.
SET max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;
SET parallel_replicas_plan_based = 1;
SET enable_parallel_replicas = 1;
-- Mode 2 always collects the statistics, without consulting the cost model.
SET automatic_parallel_replicas_mode = 2;
SET parallel_replicas_local_plan = 1;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';

SELECT key, sum(value) FROM t_pb_rename WHERE key < 100000 GROUP BY key FORMAT Null
    SETTINGS log_comment = '05361_prewhere_rename';

SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;

SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['RuntimeDataflowStatisticsInputBytes'] > 0 AS collected_statistics
FROM system.query_log
WHERE type = 'QueryFinish' AND is_initial_query AND current_database = currentDatabase()
  AND event_date >= yesterday() AND log_comment = '05361_prewhere_rename'
ORDER BY event_time_microseconds DESC
LIMIT 1;

DROP TABLE t_pb_rename;
