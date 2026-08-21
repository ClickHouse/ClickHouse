DROP TABLE IF EXISTS t_lwd_vertical;

CREATE TABLE t_lwd_vertical
(
    id UInt8,
    c1 UInt8,
    c2 UInt8,
    c3 UInt8,
    c4 UInt8,
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    min_bytes_for_wide_part = 0,
    enable_block_number_column = 0,
    enable_block_offset_column = 0,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    vertical_merge_optimize_lightweight_delete = 1,
    ratio_of_defaults_for_sparse_serialization = 1.0;

INSERT INTO t_lwd_vertical SELECT number, rand(), rand(), rand(), rand() FROM numbers(100000);

SET lightweight_delete_mode = 'alter_update';

DELETE FROM t_lwd_vertical WHERE id % 4 = 0;
SELECT count() FROM t_lwd_vertical;

OPTIMIZE TABLE t_lwd_vertical FINAL;
SELECT count() FROM t_lwd_vertical;
SELECT count() FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_lwd_vertical' AND active AND partition_id = 'all' AND column = '_row_exists';

DELETE FROM t_lwd_vertical WHERE 1;
SELECT count() FROM t_lwd_vertical;

OPTIMIZE TABLE t_lwd_vertical FINAL;
SELECT count() FROM t_lwd_vertical;
SELECT count() FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_lwd_vertical' AND active AND partition_id = 'all' AND column = '_row_exists';

SYSTEM FLUSH LOGS part_log;

SELECT
    merge_algorithm,
    read_rows,
    read_bytes,
    rows,
FROM system.part_log WHERE database = currentDatabase() AND table = 't_lwd_vertical' AND event_type = 'MergeParts'
ORDER BY event_time_microseconds;

DROP TABLE IF EXISTS t_lwd_vertical;
