-- Test: vertical insert batching by bytes creates multiple writers.
DROP TABLE IF EXISTS t_vi_batch_bytes;
SET async_insert = 0;

CREATE TABLE t_vi_batch_bytes
(
    id UInt64,
    s1 String,
    s2 String,
    s3 String,
    s4 String
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    min_rows_for_wide_part = 0,
    min_bytes_for_wide_part = 0,
    enable_vertical_insert_algorithm = 1,
    vertical_insert_algorithm_min_rows_to_activate = 1,
    vertical_insert_algorithm_min_bytes_to_activate = 0,
    vertical_insert_algorithm_min_columns_to_activate = 1,
    vertical_insert_algorithm_columns_batch_size = 1000,
    vertical_insert_algorithm_columns_batch_bytes = 1000;

INSERT INTO t_vi_batch_bytes
SELECT
    number,
    repeat('a', 500),
    repeat('b', 500),
    repeat('c', 500),
    repeat('d', 500)
FROM numbers(50);

SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['VerticalInsertWritersCreated'] > 1
FROM system.query_log
WHERE type = 'QueryFinish'
  AND query_kind = 'Insert'
  AND current_database = currentDatabase()
  AND query LIKE 'INSERT INTO t_vi_batch_bytes%'
ORDER BY event_time_microseconds DESC
LIMIT 1;

DROP TABLE t_vi_batch_bytes;
