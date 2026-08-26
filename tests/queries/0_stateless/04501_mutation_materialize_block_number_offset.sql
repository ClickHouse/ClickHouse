-- Tags: no-random-settings, no-random-merge-tree-settings, no-shared-merge-tree, no-parallel-replicas

-- Mutations materialize `_block_number` / `_block_offset` columns when they are enabled
-- and preserve the values in parts that already store them.

DROP TABLE IF EXISTS t_rewrite_block_columns;

CREATE TABLE t_rewrite_block_columns (x UInt64) ENGINE = MergeTree ORDER BY x
SETTINGS merge_selector_algorithm = 'Manual',
         enable_block_number_column = 0, enable_block_offset_column = 0,
         part_minmax_index_columns = 'partition_key_only',
         min_bytes_for_wide_part = 1, min_rows_for_wide_part = 1,
         min_bytes_for_full_part_storage = 0;

INSERT INTO t_rewrite_block_columns SELECT number FROM numbers(10); -- all_1_1_0

ALTER TABLE t_rewrite_block_columns MODIFY SETTING enable_block_number_column = 1, enable_block_offset_column = 1, part_minmax_index_columns = 'with_block_number_offset';

INSERT INTO t_rewrite_block_columns SELECT number FROM numbers(20); -- all_2_2_0
INSERT INTO t_rewrite_block_columns SELECT number FROM numbers(30); -- all_3_3_0
INSERT INTO t_rewrite_block_columns SELECT number FROM numbers(40); -- all_4_4_0

-- The merge materializes the block columns in all_2_3_1.
SYSTEM SCHEDULE MERGE t_rewrite_block_columns PARTS 'all_2_2_0', 'all_3_3_0';
SYSTEM SYNC MERGES t_rewrite_block_columns;

-- Only the merged part stores the block columns.
SELECT 'before rewrite';
SELECT name, column FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_rewrite_block_columns' AND active
ORDER BY name, column;

SELECT '';
ALTER TABLE t_rewrite_block_columns REWRITE PARTS SETTINGS mutations_sync = 2;

-- The rewrite materializes the block columns in every part.
SELECT 'after rewrite';
SELECT name, column FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_rewrite_block_columns' AND active
ORDER BY name, column;

DROP TABLE t_rewrite_block_columns;
