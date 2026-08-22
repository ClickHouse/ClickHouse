-- Tags: no-random-settings
-- The parallel replicas coordinator used to dump the whole working set (every part with all of its mark ranges)
-- into `TRACE` and `DEBUG` messages, which is megabytes of logs per query for a table with many parts.
-- Only the totals are logged now; the detailed listing is kept under the `TEST` level.

DROP TABLE IF EXISTS t_coordinator_log_volume;

-- One part per partition: the size of the old messages grew linearly with the number of parts.
CREATE TABLE t_coordinator_log_volume (k UInt32) ENGINE = MergeTree ORDER BY k PARTITION BY k;
INSERT INTO t_coordinator_log_volume SELECT number FROM numbers(150) SETTINGS max_partitions_per_insert_block = 0;

SET automatic_parallel_replicas_mode = 0;
SET enable_parallel_replicas = 1, max_parallel_replicas = 3, parallel_replicas_for_non_replicated_merge_tree = 1;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_local_plan = 0;  -- without a local plan every replica talks to the coordinator over the network
SET parallel_replicas_only_with_analyzer = 0;  -- necessary for CI run with disabled analyzer

-- Default coordinator.
SELECT count() FROM t_coordinator_log_volume WHERE NOT ignore(*)
    SETTINGS log_comment = '05028_9bd90c3e-1fd4-4d0c-8a4a-0f0f1bd6d5a9_default';

-- Reading in order coordinator.
SELECT k FROM t_coordinator_log_volume ORDER BY k LIMIT 5 OFFSET 140
    SETTINGS optimize_read_in_order = 1, log_comment = '05028_9bd90c3e-1fd4-4d0c-8a4a-0f0f1bd6d5a9_inorder';

SYSTEM FLUSH LOGS text_log, query_log;
SET max_rows_to_read = 0; -- system.text_log can be really big

-- Messages of the coordinator and of the parallel replicas read pools must not grow with the number of parts.
-- The `Test` level is excluded on purpose: the detailed listing is still allowed there.
SELECT count() > 0, max(length(message)) < 1000
FROM system.text_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND query_id IN (
        SELECT query_id FROM system.query_log
        WHERE event_date >= yesterday() AND event_time >= now() - 600
            AND current_database = currentDatabase()
            AND log_comment IN ('05028_9bd90c3e-1fd4-4d0c-8a4a-0f0f1bd6d5a9_default', '05028_9bd90c3e-1fd4-4d0c-8a4a-0f0f1bd6d5a9_inorder'))
    AND (logger_name LIKE '%Coordinator%' OR logger_name LIKE 'MergeTreeReadPoolParallelReplicas%')
    AND level != 'Test'
SETTINGS enable_parallel_replicas = 0;

DROP TABLE t_coordinator_log_volume;
