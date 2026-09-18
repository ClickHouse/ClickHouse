-- Tags: no-random-merge-tree-settings

-- Regression test: automatic parallel replicas reuses the index analysis of the single-replica plan.
-- When the OFFSET-skip read-in-order optimization has already trimmed the leading granules from that
-- analysis, it must not be reused: the parallel replicas plan is built separately and keeps the
-- original OFFSET on the initiator, so the trimmed analysis would make it skip the same rows a second time.
-- Every query below must return the same rows, and the single-replica read must stay trimmed.

DROP TABLE IF EXISTS t_skip_offset_autopr;

CREATE TABLE t_skip_offset_autopr (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 128;

INSERT INTO t_skip_offset_autopr SELECT number, number * 2 FROM numbers(5e5);

SET enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 1, parallel_replicas_local_plan = 1,
    parallel_replicas_for_non_replicated_merge_tree = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'parallel_replicas';

SET enable_analyzer = 1;

-- max_block_size is set explicitly to ensure enough blocks will be fed to the statistics collector
SET max_threads = 4, max_block_size = 128;

SET automatic_parallel_replicas_min_bytes_per_replica = 0;
SET merge_tree_min_bytes_per_task_for_remote_reading = 0;

SET optimize_read_in_order = 1, query_plan_optimize_read_in_order_skip_offset = 1;

-- Query 0: empty statistics cache, the single-replica plan runs with the analysis trimmed for the OFFSET
SELECT k, v FROM t_skip_offset_autopr ORDER BY k LIMIT 3 OFFSET 400000
    SETTINGS log_comment = '05052_read_in_order_skip_offset_automatic_parallel_replicas_query_0';

-- Query 1: whatever statistics were collected, the trimmed analysis must not be transplanted onto the parallel replicas plan
SELECT k, v FROM t_skip_offset_autopr ORDER BY k LIMIT 3 OFFSET 400000
    SETTINGS log_comment = '05052_read_in_order_skip_offset_automatic_parallel_replicas_query_1';

-- Query 2: the same query without the OFFSET-skip optimization, for reference
SELECT k, v FROM t_skip_offset_autopr ORDER BY k LIMIT 3 OFFSET 400000
    SETTINGS log_comment = '05052_read_in_order_skip_offset_automatic_parallel_replicas_query_2', query_plan_optimize_read_in_order_skip_offset = 0;

SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment AS query, read_rows < 100000 AS trimmed_read, ProfileEvents['ParallelReplicasUsedCount'] > 0 AS pr_used
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 15 MINUTE AND current_database = currentDatabase()
    AND log_comment LIKE '05052_read_in_order_skip_offset_automatic_parallel_replicas_query_%' AND type = 'QueryFinish'
ORDER BY log_comment
FORMAT TSVWithNames;

DROP TABLE t_skip_offset_autopr;
