-- `use_skip_indexes_on_data_read = 0` keeps regular skip indexes in the
-- analysis phase. TopK minmax skipping should still work at read time,
-- because it is controlled by `use_skip_indexes_for_top_k`.

SET log_queries = 1;
SET log_queries_min_type = 'QUERY_FINISH';
-- Keep random settings from disabling the TopK optimization for LIMIT 10.
SET query_plan_max_limit_for_top_k_optimization = 1000;
SET max_threads = 1;
SET max_block_size = 512;
-- Force the query through sorting, where the TopK threshold is produced.
SET optimize_read_in_order = 0;
-- Test only mark-level minmax skipping, not the row-level TopK filter.
SET use_top_k_dynamic_filtering = 0;
SET use_skip_indexes_on_data_read = 0;

DROP TABLE IF EXISTS top_k_runtime_without_data_read_setting;
CREATE TABLE top_k_runtime_without_data_read_setting
(
    id UInt64,
    keep UInt8,
    INDEX id_mm id TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id DESC
-- The runtime minmax filter skips whole marks, so fix the mark size.
SETTINGS index_granularity = 512;

INSERT INTO top_k_runtime_without_data_read_setting
SELECT number, toUInt8(number % 100 = 0)
FROM numbers(100000);

SET log_comment = '04415_top_k_runtime_without_data_read_setting_off';
SELECT id
FROM top_k_runtime_without_data_read_setting
WHERE keep = 1
ORDER BY id DESC
LIMIT 10
SETTINGS use_skip_indexes_for_top_k = 0
FORMAT Null;

SET log_comment = '04415_top_k_runtime_without_data_read_setting_on';
SELECT id
FROM top_k_runtime_without_data_read_setting
WHERE keep = 1
ORDER BY id DESC
LIMIT 10
SETTINGS use_skip_indexes_for_top_k = 1
FORMAT Null;

SET log_comment = '';
SYSTEM FLUSH LOGS query_log;

SELECT read_rows
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND query_kind = 'Select'
  AND log_comment = '04415_top_k_runtime_without_data_read_setting_off'
ORDER BY event_time_microseconds DESC
LIMIT 1;

SELECT read_rows
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND query_kind = 'Select'
  AND log_comment = '04415_top_k_runtime_without_data_read_setting_on'
ORDER BY event_time_microseconds DESC
LIMIT 1;

DROP TABLE top_k_runtime_without_data_read_setting;
