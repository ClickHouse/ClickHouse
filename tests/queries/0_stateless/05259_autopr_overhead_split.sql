-- The overhead of automatic parallel replicas is reported in parts: building the plan with parallel
-- replicas (`AutoParallelReplicasPlanBuildMicroseconds`), the rest of the optimization
-- (`AutoParallelReplicasOptimizationMicroseconds`), and collecting statistics while the query runs
-- (`RuntimeDataflowStatistics{Input,Output}Nanoseconds`). The first run has no statistics yet, so it
-- collects them; the second decides from them and collects nothing.

DROP TABLE IF EXISTS t_autopr_overhead_split;

CREATE TABLE t_autopr_overhead_split (key UInt64, val String) ENGINE = MergeTree ORDER BY key;

INSERT INTO t_autopr_overhead_split SELECT number, toString(number) FROM numbers(100000);

SET enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 1, parallel_replicas_local_plan = 1,
    parallel_replicas_for_non_replicated_merge_tree = 1, max_parallel_replicas = 3,
    automatic_parallel_replicas_min_bytes_per_replica = 0,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET enable_analyzer = 1;

SELECT key % 100 AS k, max(val) FROM t_autopr_overhead_split GROUP BY k
FORMAT Null SETTINGS log_comment = '05259_autopr_overhead_split';
SELECT key % 100 AS k, max(val) FROM t_autopr_overhead_split GROUP BY k
FORMAT Null SETTINGS log_comment = '05259_autopr_overhead_split';

SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;

SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['AutoParallelReplicasNoStatistics'] AS collected,
       ProfileEvents['AutoParallelReplicasPlanBuildMicroseconds'] > 0 AS plan_build_timed,
       ProfileEvents['AutoParallelReplicasOptimizationMicroseconds'] > 0 AS rest_timed,
       ProfileEvents['RuntimeDataflowStatisticsInputNanoseconds'] > 0 AS input_collection_timed,
       ProfileEvents['RuntimeDataflowStatisticsOutputNanoseconds'] > 0 AS output_collection_timed
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= (NOW() - toIntervalMinute(15)))
    AND (current_database = currentDatabase())
    AND (log_comment = '05259_autopr_overhead_split')
    AND (type = 'QueryFinish') AND is_initial_query
ORDER BY event_time_microseconds;

DROP TABLE t_autopr_overhead_split;
