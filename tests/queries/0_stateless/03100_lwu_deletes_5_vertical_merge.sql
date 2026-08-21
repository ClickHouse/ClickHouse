DROP TABLE IF EXISTS t_lwu_deletes_vertical;

CREATE TABLE t_lwu_deletes_vertical
(
    id UInt64,
    c1 UInt64,
    c2 UInt64,
    c3 String,
    c4 String
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    min_bytes_for_wide_part = 0,
    enable_block_number_column = 1,
    enable_block_offset_column = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    vertical_merge_optimize_lightweight_delete = 1;

INSERT INTO t_lwu_deletes_vertical SELECT number, rand(), rand(), randomPrintableASCII(10), randomPrintableASCII(10) FROM numbers(100000);

SET enable_lightweight_update = 1;
SET lightweight_delete_mode = 'lightweight_update_force';

DELETE FROM t_lwu_deletes_vertical WHERE id % 4 = 0;
SELECT count() FROM t_lwu_deletes_vertical;

OPTIMIZE TABLE t_lwu_deletes_vertical FINAL;
SELECT count() FROM t_lwu_deletes_vertical;
SELECT count() FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_lwu_deletes_vertical' AND active AND partition_id = 'all' AND column = '_row_exists';

DELETE FROM t_lwu_deletes_vertical WHERE 1;
SELECT count() FROM t_lwu_deletes_vertical;

OPTIMIZE TABLE t_lwu_deletes_vertical FINAL;
SELECT count() FROM t_lwu_deletes_vertical;
SELECT count() FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_lwu_deletes_vertical' AND active AND partition_id = 'all' AND column = '_row_exists';

SYSTEM FLUSH LOGS part_log;

SELECT
    merge_algorithm,
    read_rows,
    rows,
    ProfileEvents['ReadTasksWithAppliedPatches'],
    ProfileEvents['PatchesReadRows']
FROM system.part_log WHERE database = currentDatabase() AND table = 't_lwu_deletes_vertical' AND event_type = 'MergeParts'
ORDER BY event_time_microseconds;

DROP TABLE IF EXISTS t_lwu_deletes_vertical;
