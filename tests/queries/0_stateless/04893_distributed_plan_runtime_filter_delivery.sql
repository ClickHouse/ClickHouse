-- Tags: no-old-analyzer

CREATE TABLE big (bid UInt64, v UInt64) ENGINE = MergeTree ORDER BY bid SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
CREATE TABLE small (sid UInt64, name String) ENGINE = MergeTree ORDER BY sid;
INSERT INTO big SELECT number, number FROM numbers(1000000);
INSERT INTO small SELECT number * 100, toString(number) FROM numbers(100);

SET enable_analyzer = 1, enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0, enable_parallel_replicas = 0;
SET make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_plan_max_rows_to_broadcast = 0;
SET max_rows_to_group_by = 0, query_plan_join_swap_table = 0, query_plan_optimize_join_order_randomize = 0;
SET log_processors_profiles = 1;
SET distributed_plan_join_runtime_filters = 1;

SELECT count() FROM big, small WHERE bid = sid SETTINGS log_comment = '04893_runtime_filter_delivery';

SET make_distributed_plan = 0;

-- `BuildRuntimeFilterPartialTransform` with `output_rows > input_rows` shows that a build task
-- sent its filter state to an exchange (it is counted at the sender). A local filter does not
-- show it, and here both build tasks must send. The receiver is not checked: a probe task stops waiting for the
-- filter when its data work is done, so the filter may not arrive.
SYSTEM FLUSH LOGS query_log, processors_profile_log;
SELECT countIf(name = 'BuildRuntimeFilterPartialTransform' AND output_rows > input_rows) >= 2
FROM system.processors_profile_log
WHERE event_date >= yesterday()
  AND query_id IN (
      SELECT query_id FROM system.query_log
      WHERE type = 'QueryFinish' AND event_date >= yesterday()
        AND initial_query_id IN (
            SELECT query_id FROM system.query_log
            WHERE type = 'QueryFinish' AND is_initial_query AND event_date >= yesterday()
              AND current_database = currentDatabase() AND log_comment = '04893_runtime_filter_delivery'));
