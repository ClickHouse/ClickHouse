-- Tags: no-old-analyzer

-- Broadcast dim next to local_probe; remote_probe stays behind a shuffle. Same-stage apply
-- used to fail-open once the producer also shipped a filter. Dual-mode registers after
-- serialize so that apply still prunes.

CREATE TABLE dim (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE local_probe (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE remote_probe (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO dim SELECT number FROM numbers(10);
INSERT INTO local_probe SELECT number FROM numbers(100000);
INSERT INTO remote_probe SELECT number FROM numbers(100000);

SET enable_analyzer = 1, enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0, enable_parallel_replicas = 0;
SET enable_join_runtime_filters_index_analysis = 0;
SET make_distributed_plan = 1, distributed_plan_execute_locally = 1;
SET query_plan_join_swap_table = 0, query_plan_optimize_join_order_randomize = 0;
SET query_plan_optimize_join_order_algorithm = 'greedy', query_plan_optimize_join_order_limit = 10, use_hash_table_stats_for_join_reordering = 0, use_statistics = 0;
SET max_rows_to_group_by = 0;
SET distributed_plan_join_runtime_filters = 1, distributed_plan_max_rows_to_broadcast = 100;

SELECT '-- mixed local+remote apply';
SELECT count()
FROM local_probe AS p
INNER JOIN dim AS d ON p.id = d.id
INNER JOIN remote_probe AS r ON p.id = r.id
SETTINGS log_comment = '04948_mixed';

SET make_distributed_plan = 0;
SYSTEM FLUSH LOGS query_log, text_log;

-- Fail-open never calls find(), so it never logs a Stats line with checked > passed.
SELECT '-- local apply pruned the same-stage probe';
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

-- A transported filter is registered under one union key on every consuming task.
SELECT '-- remote apply still arrived over the exchange';
SELECT count() >= 1
FROM
(
    SELECT extract(message, 'under key \'([^\']+)\'') AS filter_key
    FROM system.text_log
    WHERE logger_name = 'RuntimeFilter' AND event_date >= yesterday()
      AND message LIKE 'Registered runtime filter%'
      AND query_id IN (
          SELECT query_id FROM system.query_log
          WHERE type = 'QueryFinish' AND event_date >= yesterday()
            AND initial_query_id IN (
                SELECT query_id FROM system.query_log
                WHERE type = 'QueryFinish' AND is_initial_query AND event_date >= yesterday()
                  AND current_database = currentDatabase() AND log_comment = '04948_mixed')
            AND (query LIKE 'stage_%' OR query LIKE 'rf_merge_%'))
    GROUP BY filter_key
    HAVING count() >= 2
);
