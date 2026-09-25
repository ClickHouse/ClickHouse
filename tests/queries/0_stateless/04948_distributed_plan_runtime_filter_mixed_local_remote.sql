-- Tags: no-old-analyzer

-- `dim` is scanned in a stage of its own. That stage builds the filter for `p.id = d.id` and ships
-- it through a merge tree to the stage that scans `local_probe`. `dim` is also broadcast into that
-- stage for the join, and the stage derives its own local filter from the broadcast rows. The join
-- with `remote_probe` goes through a shuffle, and transport of its filter is refused. So one plan
-- mixes a transported filter, a stage-local one and a refused one.

CREATE TABLE dim (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE local_probe (id UInt64) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
CREATE TABLE remote_probe (id UInt64) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
INSERT INTO dim SELECT number FROM numbers(10);
INSERT INTO local_probe SELECT number FROM numbers(100000);
INSERT INTO remote_probe SELECT number FROM numbers(100000);

SET enable_analyzer = 1, enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0, enable_parallel_replicas = 0;
SET enable_join_runtime_filters_index_analysis = 0;
SET make_distributed_plan = 1, distributed_plan_execute_locally = 1;
SET query_plan_join_swap_table = 0, query_plan_optimize_join_order_randomize = 0;
SET query_plan_optimize_join_order_algorithm = 'greedy', query_plan_optimize_join_order_limit = 10, use_hash_table_stats_for_join_reordering = 0, use_statistics = 0;
SET max_rows_to_group_by = 0, log_processors_profiles = 1;
SET distributed_plan_join_runtime_filters = 1, distributed_plan_max_rows_to_broadcast = 100;

SELECT '-- mixed local+remote apply';
SELECT count()
FROM local_probe AS p
INNER JOIN dim AS d ON p.id = d.id
INNER JOIN remote_probe AS r ON p.id = r.id
SETTINGS log_comment = '04948_mixed';

SET make_distributed_plan = 0;
SYSTEM FLUSH LOGS query_log, text_log, processors_profile_log;

-- Two filters apply to the `local_probe` scan: the shipped one and the stage's own one, built from
-- the broadcast `dim`. So the scan is pruned whether or not the shipped filter arrives first. Even
-- without transport, the stage's own filter would prune it alone. A fail-open `__applyFilter` never
-- calls `RuntimeFilter::find`, so it leaves no `Stats for` line with fewer rows passed than
-- checked.
SELECT '-- local_probe pruned in its own stage';
SELECT count() >= 1
FROM system.text_log
WHERE logger_name = 'RuntimeFilter' AND event_date >= yesterday()
  AND message LIKE 'Stats for%'
  AND toUInt64OrZero(extract(message, 'rows checked (\\d+)')) >= 100
  AND toUInt64OrZero(extract(message, 'rows passed (\\d+)'))
      < toUInt64OrZero(extract(message, 'rows checked (\\d+)'))
  AND query_id IN (
      SELECT query_id FROM system.query_log
      WHERE type = 'QueryFinish' AND event_date >= yesterday()
        AND initial_query_id IN (
            SELECT query_id FROM system.query_log
            WHERE type = 'QueryFinish' AND is_initial_query AND event_date >= yesterday()
              AND current_database = currentDatabase() AND log_comment = '04948_mixed')
        AND (query LIKE 'stage_%' OR query LIKE 'rf_merge_%'));

-- The filter for `p.id = d.id` is transported, so check that its producer shipped it.
-- `BuildRuntimeFilterPartialTransform` serializes the build task's partial and appends it to the
-- task's exchange sink as one extra row, so `output_rows > input_rows` marks a task that put a
-- state on an exchange. It exists only on the transported path: a filter that stays local is
-- built by `BuildRuntimeFilterTransform`, which appears in equal numbers either way.
--
-- It does not assert that the `local_probe` stage received the state: the merge -> probe
-- broadcast is best-effort by design, because a probe task cancels its receive branch once its
-- data work is done.
SELECT '-- the producer also shipped the filter over the exchange';
SELECT countIf(name = 'BuildRuntimeFilterPartialTransform' AND output_rows > input_rows) >= 1
FROM system.processors_profile_log
WHERE event_date >= yesterday()
  AND query_id IN (
      SELECT query_id FROM system.query_log
      WHERE type = 'QueryFinish' AND event_date >= yesterday()
        AND initial_query_id IN (
            SELECT query_id FROM system.query_log
            WHERE type = 'QueryFinish' AND is_initial_query AND event_date >= yesterday()
              AND current_database = currentDatabase() AND log_comment = '04948_mixed'));
