-- Test: vertical insert with delayed streams disabled (max_insert_delayed_streams_for_parallel_write=0).
SET materialize_skip_indexes_on_insert = 1;
SET async_insert = 0;

DROP TABLE IF EXISTS t_vi_delayed_streams_off;

CREATE TABLE t_vi_delayed_streams_off
(
    id UInt64,
    c1 UInt64,
    c2 UInt64,
    c3 UInt64,
    c4 UInt64,
    c5 UInt64,
    c6 UInt64,
    c7 UInt64,
    c8 UInt64,
    c9 UInt64,
    c10 UInt64,
    INDEX c1_bf c1 TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    min_rows_for_wide_part = 0,
    min_bytes_for_wide_part = 0,
    index_granularity = 1,
    fsync_after_insert = 1,
    enable_vertical_insert_algorithm = 1,
    vertical_insert_algorithm_min_rows_to_activate = 1,
    vertical_insert_algorithm_min_bytes_to_activate = 0,
    vertical_insert_algorithm_min_columns_to_activate = 1,
    vertical_insert_algorithm_columns_batch_size = 1;

SET max_insert_delayed_streams_for_parallel_write = 0;

INSERT INTO t_vi_delayed_streams_off
SELECT number, number, number, number, number, number, number, number, number, number, number
FROM numbers(1000);

SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['VerticalInsertWriterFlushes'] > 0
FROM system.query_log
WHERE type = 'QueryFinish'
  AND query_kind = 'Insert'
  AND current_database = currentDatabase()
  AND Settings['max_insert_delayed_streams_for_parallel_write'] = '0'
  AND query LIKE 'INSERT INTO t_vi_delayed_streams_off%'
ORDER BY event_time_microseconds DESC
LIMIT 1;

SELECT count()
FROM system.parts
WHERE database = currentDatabase()
  AND table = 't_vi_delayed_streams_off'
  AND active
  AND secondary_indices_uncompressed_bytes > 0;

DROP TABLE t_vi_delayed_streams_off;
