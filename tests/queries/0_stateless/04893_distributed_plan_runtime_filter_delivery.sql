-- Tags: no-old-analyzer

CREATE TABLE big (bid UInt64, v UInt64) ENGINE = MergeTree ORDER BY bid;
CREATE TABLE small (sid UInt64, name String) ENGINE = MergeTree ORDER BY sid;
INSERT INTO big SELECT number, number FROM numbers(1000000);
INSERT INTO small SELECT number * 100, toString(number) FROM numbers(100);

SET enable_analyzer = 1, enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0, enable_parallel_replicas = 0;
SET make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_plan_max_rows_to_broadcast = 0;
SET max_rows_to_group_by = 0, query_plan_join_swap_table = 0, query_plan_optimize_join_order_randomize = 0;
SET log_processors_profiles = 1;
SET distributed_plan_join_runtime_filters = 1;

SELECT count() FROM big, small WHERE bid = sid SETTINGS log_comment = '04511_runtime_filter_delivery';

SET make_distributed_plan = 0;

-- Whether the partials reached an exchange is read from `system.processors_profile_log`:
-- `BuildRuntimeFilterPartialTransform` serializes a build task's partial and appends it to the
-- task's exchange sink as one extra row, so `output_rows > input_rows` marks a task that put a
-- state on an exchange. It exists only on the transported path - a filter that stays local is
-- built by `BuildRuntimeFilterTransform`, which is present in equal numbers with the setting on
-- and off - so `>= 2` means two separate build tasks shipped a state onto the shared filter's
-- exchange, and a local filter scores 0. The count is taken where the state is serialized, so it
-- does not depend on whether anything received it.
--
-- The previous form counted `RuntimeFilter` log lines carried by the probe-scan tasks, and did
-- not work as written: with `distributed_plan_execute_locally` every task of the query shares the
-- initiator's `query_id`, so the `stage_0_%` restriction selected nothing and any log line from
-- any task satisfied it - including the ones a purely local filter writes, which is why the check
-- also passed with `distributed_plan_join_runtime_filters = 0`. Even taken at face value it
-- asserted the merge -> probe broadcast, which is best-effort by design: a probe task cancels its
-- receive branch once its data work is done, so an arrival count is not a property of this code.
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
              AND current_database = currentDatabase() AND log_comment = '04511_runtime_filter_delivery'));
