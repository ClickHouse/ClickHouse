-- Verify that max_parallel_replicas is capped to the actual cluster size.
-- With max_parallel_replicas=1000 (uncapped), the formula would favor parallel replicas.
-- But since the cluster has only 3 nodes, num_replicas is capped to 3 and the formula
-- should NOT favor parallel replicas for a query where output_bytes ≈ input_bytes.

DROP TABLE IF EXISTS t;

CREATE TABLE t(key UInt64, value UInt64) ENGINE = MergeTree ORDER BY tuple();

-- max_parallel_replicas=1000 is intentionally much larger than the cluster size (3 replicas).
-- merge_tree_min_bytes_per_task_for_remote_reading=0 disables the effective reading threads cap
-- so the formula simplifies to: input / max_threads vs input / (max_threads * num_replicas) + output / num_replicas.
SET enable_parallel_replicas=1, automatic_parallel_replicas_mode=1, parallel_replicas_local_plan=1, parallel_replicas_index_analysis_only_on_coordinator=1,
    parallel_replicas_for_non_replicated_merge_tree=1, max_parallel_replicas=1000, cluster_for_parallel_replicas='test_cluster_one_shard_three_replicas_localhost';

SET enable_analyzer=1;
SET max_threads=4;
SET max_bytes_before_external_group_by=0, max_bytes_ratio_before_external_group_by=0;

-- To not let other heuristics interfere with the logic we test
SET merge_tree_min_bytes_per_task_for_remote_reading=0;
SET automatic_parallel_replicas_min_bytes_per_replica=0;

-- Many unique keys so output_bytes ≈ input_bytes after GROUP BY.
-- With num_replicas=3: replicas_cost = input/12 + output/3 ≈ input*5/12 > input/4 = local_cost → PR not enabled.
-- With num_replicas=1000 (uncapped): replicas_cost ≈ input/4000 + output/1000 ≈ 0 < input/4 → PR would be enabled.
INSERT INTO t SELECT number, number FROM numbers(5e5);

-- First query: collect statistics (cache is empty)
SELECT key, value FROM t GROUP BY key, value FORMAT Null SETTINGS log_comment='03837_autopr_max_parallel_replicas_capped_query_0';

-- Second query: statistics available, formula is evaluated using capped num_replicas=3.
-- Parallel replicas should NOT be enabled because with only 3 replicas the cost is higher.
SELECT key, value FROM t GROUP BY key, value FORMAT Null SETTINGS log_comment='03837_autopr_max_parallel_replicas_capped_query_1';

SET enable_parallel_replicas=0, automatic_parallel_replicas_mode=0;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, ProfileEvents['RuntimeDataflowStatisticsInputBytes'] > 0 AS stats_collected, ProfileEvents['ParallelReplicasUsedCount'] > 0 AS pr_used
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= (NOW() - toIntervalMinute(15))) AND (current_database = currentDatabase()) AND (log_comment LIKE '03837_autopr_max_parallel_replicas_capped_query_%') AND (type = 'QueryFinish')
ORDER BY log_comment
FORMAT TSVWithNames;

DROP TABLE t;
