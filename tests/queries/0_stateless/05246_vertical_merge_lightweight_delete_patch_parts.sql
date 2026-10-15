-- A vertical merge of patch parts written by lightweight deletes keeps every row: rows with `_row_exists = 0` are
-- payload. Counting them as deleted rows aborts the merge with LOGICAL_ERROR "past its last row".

DROP TABLE IF EXISTS t_lwd_vertical_patch;

CREATE TABLE t_lwd_vertical_patch
(
    id UInt64,
    c1 String
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    min_bytes_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0,
    enable_block_number_column = 1,
    enable_block_offset_column = 1,
    patch_parts_version = 'v2',
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    -- only the explicit OPTIMIZE FINAL (which ignores this limit) may merge the patch parts
    max_bytes_to_merge_at_max_space_in_pool = 1;

SET enable_lightweight_update = 1;
SET lightweight_delete_mode = 'lightweight_update_force';

INSERT INTO t_lwd_vertical_patch SELECT number, toString(number) FROM numbers(10);

DELETE FROM t_lwd_vertical_patch WHERE id IN (0, 1);
DELETE FROM t_lwd_vertical_patch WHERE id IN (2, 3);

OPTIMIZE TABLE t_lwd_vertical_patch PARTITION ID 'patch-9d3c00135c25f28efda622e6c7308f8a-all' FINAL;

SELECT 'patch parts', count(), sum(rows)
FROM system.parts
WHERE database = currentDatabase() AND table = 't_lwd_vertical_patch' AND active AND startsWith(name, 'patch');

SELECT 'visible rows', count(), sum(id) FROM t_lwd_vertical_patch;

SYSTEM FLUSH LOGS part_log;
SELECT 'merge algorithm', merge_algorithm
FROM system.part_log
WHERE database = currentDatabase() AND table = 't_lwd_vertical_patch' AND event_type = 'MergeParts';

DROP TABLE t_lwd_vertical_patch;
