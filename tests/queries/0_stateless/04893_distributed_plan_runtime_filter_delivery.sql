-- Tags: no-old-analyzer

CREATE TABLE big (bid UInt64, v UInt64) ENGINE = MergeTree ORDER BY bid;
CREATE TABLE small (sid UInt64, name String) ENGINE = MergeTree ORDER BY sid;
INSERT INTO big SELECT number, number FROM numbers(1000000);
INSERT INTO small SELECT number * 100, toString(number) FROM numbers(100);

SET enable_analyzer = 1, enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0, enable_parallel_replicas = 0;
SET make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_plan_max_rows_to_broadcast = 0;
SET max_rows_to_group_by = 0, query_plan_join_swap_table = 0, query_plan_optimize_join_order_randomize = 0;
SET distributed_plan_join_runtime_filters = 1;

SELECT count() FROM big, small WHERE bid = sid SETTINGS log_comment = '04511_runtime_filter_delivery';

SET make_distributed_plan = 0;

-- The probe-scan tasks (stage_0) hold only the filter that arrived over the exchange, so a stats
-- line in their contexts proves the partials were delivered, merged and registered. The build side
-- is tiny, so the filter arrives while the probe tasks still run.
SYSTEM FLUSH LOGS query_log, text_log;
SELECT count() > 0 FROM system.text_log
WHERE logger_name = 'RuntimeFilter' AND query_id IN (
    SELECT query_id FROM system.query_log
    WHERE type = 'QueryFinish' AND query LIKE 'stage_0_%' AND initial_query_id IN (
        SELECT query_id FROM system.query_log
        WHERE type = 'QueryFinish' AND is_initial_query AND log_comment = '04511_runtime_filter_delivery'
            AND current_database = currentDatabase() AND event_date >= yesterday())
        AND event_date >= yesterday())
    AND event_date >= yesterday();
