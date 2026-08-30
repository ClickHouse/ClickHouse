-- Tags: no-random-settings
-- The parallel replicas coordinator used to dump the whole working set (every part with all of its mark ranges)
-- into `TRACE` and `DEBUG` messages, which is megabytes of logs per query for a large working set.
-- Only the totals are logged now; the detailed listing is kept under the `TEST` level.

DROP TABLE IF EXISTS t_coordinator_log_volume;

-- The size of the old messages grew linearly with the number of parts and with the number of mark ranges in
-- every part. `index_granularity = 1` turns every row into a separate granule, and a condition on the second
-- component of the primary key keeps only every fourth of them, which gives a working set of hundreds of
-- non-adjacent ranges without writing a table that is expensive to create.
CREATE TABLE t_coordinator_log_volume (a UInt32, b UInt8) ENGINE = MergeTree ORDER BY (a, b)
    PARTITION BY intDiv(a, 75) SETTINGS index_granularity = 1;
INSERT INTO t_coordinator_log_volume SELECT intDiv(number, 4), number % 4 FROM numbers(1200);

SET automatic_parallel_replicas_mode = 0;
SET enable_parallel_replicas = 1, max_parallel_replicas = 3, parallel_replicas_for_non_replicated_merge_tree = 1;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_local_plan = 0;  -- without a local plan every replica talks to the coordinator over the network
SET parallel_replicas_only_with_analyzer = 0;  -- necessary for CI run with disabled analyzer

-- Default coordinator.
SELECT count() FROM t_coordinator_log_volume WHERE b = 0 AND NOT ignore(*)
    SETTINGS log_comment = '05029_9bd90c3e-1fd4-4d0c-8a4a-0f0f1bd6d5a9_default';

-- Reading in order coordinator.
SELECT a FROM t_coordinator_log_volume WHERE b = 0 ORDER BY a LIMIT 5 OFFSET 290
    SETTINGS optimize_read_in_order = 1, log_comment = '05029_9bd90c3e-1fd4-4d0c-8a4a-0f0f1bd6d5a9_inorder';

SYSTEM FLUSH LOGS text_log, query_log;

-- The queries below only look at what has been logged, they must not use parallel replicas themselves.
SET enable_parallel_replicas = 0;
SET max_rows_to_read = 0; -- system.text_log can be really big

-- The check below is meaningless unless the queries really had a large working set. The counters are only
-- reported by the replicas, the initiator does not read anything on its own without a local plan.
SELECT max(ProfileEvents['SelectedParts']) >= 4, max(ProfileEvents['SelectedRanges']) >= 100
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND type = 'QueryFinish'
    AND initial_query_id IN (
        SELECT query_id FROM system.query_log
        WHERE event_date >= yesterday() AND event_time >= now() - 600
            AND current_database = currentDatabase()
            AND log_comment IN ('05029_9bd90c3e-1fd4-4d0c-8a4a-0f0f1bd6d5a9_default', '05029_9bd90c3e-1fd4-4d0c-8a4a-0f0f1bd6d5a9_inorder'));

-- Messages of the coordinator and of the parallel replicas read pools must not grow with the size of the working set.
-- The `Test` level is excluded on purpose: the detailed listing is still allowed there.
-- Without a local plan the read pool messages are logged by the parallel replica worker queries, and
-- `system.text_log` only stores their own `query_id`, so the filter has to cover every query whose
-- `initial_query_id` points to the two top-level queries, not just the top-level queries themselves.
SELECT count() > 0, max(length(message)) < 1000
FROM system.text_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND query_id IN (
        SELECT query_id FROM system.query_log
        WHERE event_date >= yesterday() AND event_time >= now() - 600
            AND initial_query_id IN (
                SELECT query_id FROM system.query_log
                WHERE event_date >= yesterday() AND event_time >= now() - 600
                    AND current_database = currentDatabase()
                    AND log_comment IN ('05029_9bd90c3e-1fd4-4d0c-8a4a-0f0f1bd6d5a9_default', '05029_9bd90c3e-1fd4-4d0c-8a4a-0f0f1bd6d5a9_inorder')))
    AND (logger_name LIKE '%Coordinator%' OR logger_name LIKE 'MergeTreeReadPoolParallelReplicas%')
    AND level != 'Test';

DROP TABLE t_coordinator_log_volume;
