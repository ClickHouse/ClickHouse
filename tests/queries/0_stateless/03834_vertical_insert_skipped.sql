-- Test: vertical insert skipped when disabled and horizontal path is used.
SET materialize_skip_indexes_on_insert = 1;
SET async_insert = 0;

DROP TABLE IF EXISTS t_vi_skipped;

CREATE TABLE t_vi_skipped
(
    id UInt64,
    v UInt64
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    min_rows_for_wide_part = 0,
    min_bytes_for_wide_part = 0,
    index_granularity = 1,
    enable_vertical_insert_algorithm = 0,
    vertical_insert_algorithm_min_rows_to_activate = 1,
    vertical_insert_algorithm_min_bytes_to_activate = 0,
    vertical_insert_algorithm_min_columns_to_activate = 1;

INSERT INTO t_vi_skipped
SELECT number, number FROM numbers(10);

SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['VerticalInsertSkipped'] >= 1
FROM system.query_log
WHERE type = 'QueryFinish'
  AND query_kind = 'Insert'
  AND current_database = currentDatabase()
  AND query LIKE 'INSERT INTO t_vi_skipped%'
ORDER BY event_time_microseconds DESC
LIMIT 1;

DROP TABLE t_vi_skipped;
