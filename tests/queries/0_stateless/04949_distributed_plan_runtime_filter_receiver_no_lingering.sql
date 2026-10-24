-- Tags: no-old-analyzer

-- Local disk: an object-storage disk can stall a read for seconds, and a slow probe task would look like one that waited for the filter.
CREATE TABLE big (bid UInt64, v UInt64) ENGINE = MergeTree ORDER BY bid SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi', storage_policy = 'default';
CREATE TABLE small (sid UInt64, name String) ENGINE = MergeTree ORDER BY sid SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi', storage_policy = 'default';
INSERT INTO big SELECT number, number FROM numbers(100000);
INSERT INTO small SELECT number * 100, toString(number) FROM numbers(100);

SET enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0, enable_parallel_replicas = 0;
SET make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_plan_max_rows_to_broadcast = 0;
SET max_rows_to_group_by = 0, query_plan_join_swap_table = 0, query_plan_optimize_join_order_randomize = 0;
SET distributed_plan_join_runtime_filters = 1, function_sleep_max_microseconds_per_block = 20000000;

-- The build side sleeps about 10 seconds before the filter is ready. The probe tasks finish their
-- data work in milliseconds and must not wait for the filter. The long sleep leaves a large margin
-- for slow probe tasks on a busy machine (measured 55 ms idle, 432 ms under heavy load).
SELECT count() FROM big, (SELECT sid FROM small WHERE sleepEachRow(0.1) = 0) AS s WHERE bid = s.sid
SETTINGS log_comment = '04949_no_lingering';

SET make_distributed_plan = 0;

SYSTEM FLUSH LOGS query_log;

-- 1. The build task (stage_1) did sleep. Otherwise a slow probe task could not be told apart.
-- 2. An `rf_merge_%` task exists, so the probe tasks do have a filter to wait for.
-- 3. The probe tasks (stage_0) finish much faster than the build task. A probe task that waited
--    for the filter would take as long as the sleep. Durations are compared, not end times,
--    because a probe task can start late on a busy machine.
-- Each check also requires rows, because `maxIf` over no rows returns 0. Tasks are found through
-- `initial_query_id` of the root query, which has the current database.
SELECT countIf(query LIKE 'stage_1_%') > 0 AND maxIf(query_duration_ms, query LIKE 'stage_1_%') > 5000,
       countIf(query LIKE 'rf_merge_%') > 0,
       countIf(query LIKE 'stage_0_%') > 0
           AND maxIf(query_duration_ms, query LIKE 'stage_0_%') * 2 < maxIf(query_duration_ms, query LIKE 'stage_1_%')
FROM system.query_log
WHERE type = 'QueryFinish' AND event_date >= yesterday() AND query NOT LIKE 'SELECT%'
    AND initial_query_id IN (
        SELECT query_id FROM system.query_log
        WHERE type = 'QueryFinish' AND is_initial_query AND log_comment = '04949_no_lingering'
            AND current_database = currentDatabase() AND event_date >= yesterday());
