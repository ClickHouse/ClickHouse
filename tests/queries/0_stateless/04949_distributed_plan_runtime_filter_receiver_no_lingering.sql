-- Tags: no-old-analyzer

CREATE TABLE big (bid UInt64, v UInt64) ENGINE = MergeTree ORDER BY bid;
CREATE TABLE small (sid UInt64, name String) ENGINE = MergeTree ORDER BY sid;
INSERT INTO big SELECT number, number FROM numbers(100000);
INSERT INTO small SELECT number * 100, toString(number) FROM numbers(100);

SET enable_analyzer = 1, enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0, enable_parallel_replicas = 0;
SET make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_plan_max_rows_to_broadcast = 0;
SET max_rows_to_group_by = 0, query_plan_join_swap_table = 0, query_plan_optimize_join_order_randomize = 0;
SET distributed_plan_join_runtime_filters = 1, function_sleep_max_microseconds_per_block = 6000000;

-- The build side sleeps ~5 seconds before the runtime filter can be built. The probe-scan tasks
-- finish their data work in milliseconds and must not stay alive waiting for the filter.
SELECT count() FROM big, (SELECT sid FROM small WHERE sleepEachRow(0.05) = 0) AS s WHERE bid = s.sid
SETTINGS log_comment = '04949_no_lingering';

SET make_distributed_plan = 0;

SYSTEM FLUSH LOGS query_log;

-- The probe tasks (stage_0) must finish much earlier than the build-scan task that holds the
-- sleep (stage_1). Without the receive-branch cancellation a probe task lives until the filter
-- arrives, which cannot happen before the sleep ends, and both sides show the same duration.
-- With local execution every task fragment shares the root query's `query_id`, so the fragments
-- are selected through the root query, which carries the current database.
SELECT maxIf(query_duration_ms, query LIKE 'stage_0_%') * 2 < maxIf(query_duration_ms, query LIKE 'stage_1_%')
FROM system.query_log
WHERE type = 'QueryFinish' AND event_date >= yesterday() AND query NOT LIKE 'SELECT%'
    AND query_id IN (
        SELECT query_id FROM system.query_log
        WHERE type = 'QueryFinish' AND is_initial_query AND log_comment = '04949_no_lingering'
            AND current_database = currentDatabase() AND event_date >= yesterday());
