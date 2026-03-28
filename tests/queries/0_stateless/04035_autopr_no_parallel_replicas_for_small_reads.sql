-- Verify that automatic parallel replicas are not enabled for queries that read very little data.
-- The effective_max_reading_threads cap (based on merge_tree_min_bytes_per_task_for_remote_reading)
-- ensures that when input_bytes is small, local execution is recognized as already optimal.
-- Without this cap, dividing input_bytes by max_threads would make local_cost appear artificially
-- small, potentially triggering parallel replicas for queries that don't benefit from them.

DROP TABLE IF EXISTS t;

CREATE TABLE t(key UInt64, value String) ENGINE = MergeTree ORDER BY key;

SET enable_parallel_replicas=1, automatic_parallel_replicas_mode=1, parallel_replicas_local_plan=1, parallel_replicas_index_analysis_only_on_coordinator=1,
    parallel_replicas_for_non_replicated_merge_tree=1, max_parallel_replicas=3, cluster_for_parallel_replicas='test_cluster_one_shard_three_replicas_localhost';

SET enable_analyzer=1;
SET max_threads=4;
SET max_bytes_before_external_group_by=0, max_bytes_ratio_before_external_group_by=0;
SET automatic_parallel_replicas_min_bytes_per_replica=0;

INSERT INTO t SELECT number, toString(number) FROM numbers(1e6);

-- Query 0: point lookup, collect statistics (cache is empty).
-- Only a handful of rows are read thanks to the primary key index.
SELECT key FROM t WHERE key = 42 FORMAT Null SETTINGS log_comment='04035_autopr_no_pr_for_small_reads_query_0';

-- Query 1: same point lookup, statistics available.
-- input_bytes is tiny (< merge_tree_min_bytes_per_task_for_remote_reading), so effective_max_reading_threads = 1,
-- meaning local_cost = input_bytes and replicas_cost = input_bytes + output_bytes/3 > local_cost.
-- Parallel replicas should NOT be enabled.
SELECT key FROM t WHERE key = 42 FORMAT Null SETTINGS log_comment='04035_autopr_no_pr_for_small_reads_query_1';

SET enable_parallel_replicas=0, automatic_parallel_replicas_mode=0;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, ProfileEvents['RuntimeDataflowStatisticsInputBytes'] > 0 AS stats_collected, ProfileEvents['ParallelReplicasUsedCount'] > 0 AS pr_used
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= (NOW() - toIntervalMinute(15))) AND (current_database = currentDatabase()) AND (log_comment LIKE '04035_autopr_no_pr_for_small_reads_query_%') AND (type = 'QueryFinish')
ORDER BY log_comment
FORMAT TSVWithNames;

DROP TABLE t;
