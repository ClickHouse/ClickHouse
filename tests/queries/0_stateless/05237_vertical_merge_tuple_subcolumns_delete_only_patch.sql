-- Tags: no-random-merge-tree-settings, no-replicated-database, no-parallel-replicas

SET enable_lightweight_update = 1;
SET lightweight_delete_mode = 'lightweight_update_force';

DROP TABLE IF EXISTS t_tuple_delete_only_patch SYNC;

CREATE TABLE t_tuple_delete_only_patch
(
    k UInt64,
    t Tuple(x String, y String)
)
ENGINE = MergeTree
ORDER BY k
SETTINGS
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0,
    enable_block_number_column = 1,
    enable_block_offset_column = 1,
    apply_patches_on_merge = 1,
    enable_vertical_merge_algorithm = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 0,
    vertical_merge_algorithm_min_columns_to_activate = 2,
    allow_experimental_vertical_merge_tuple_subcolumns = 1,
    auto_statistics_types = '';

SYSTEM STOP MERGES t_tuple_delete_only_patch;

INSERT INTO t_tuple_delete_only_patch VALUES
    (1, ('a', 'b')),
    (2, ('c', 'd'));
INSERT INTO t_tuple_delete_only_patch VALUES
    (3, ('e', 'f')),
    (4, ('g', 'h'));

DELETE FROM t_tuple_delete_only_patch WHERE k IN (2, 4);

SELECT 'delete_only_patch',
       countIf(column = '_row_exists') > 0 AND countIf(column = 't') = 0
FROM system.parts_columns
WHERE database = currentDatabase()
  AND table = 't_tuple_delete_only_patch'
  AND active
  AND startsWith(name, 'patch');

SYSTEM START MERGES t_tuple_delete_only_patch;
OPTIMIZE TABLE t_tuple_delete_only_patch FINAL;
SYSTEM FLUSH LOGS part_log;

SELECT 'merge_algorithm', merge_algorithm
FROM system.part_log
WHERE database = currentDatabase()
  AND table = 't_tuple_delete_only_patch'
  AND event_type = 'MergeParts'
ORDER BY event_time_microseconds DESC
LIMIT 1;

SELECT k, t FROM t_tuple_delete_only_patch ORDER BY k;

DROP TABLE t_tuple_delete_only_patch SYNC;
