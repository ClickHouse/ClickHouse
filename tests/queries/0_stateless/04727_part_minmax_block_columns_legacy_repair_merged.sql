-- Tags: no-shared-merge-tree
-- no-shared-merge-tree: RMT/SMT allocate block numbers starting from 0

-- Like 04671_part_minmax_block_columns_legacy_repair, but for a part that covers more than one block: a
-- part merged while `part_minmax_index_columns` did not cover `_block_number` has no
-- `minmax__block_number.idx` either, so after the setting is widened the range comes back as the whole
-- universe. A later column-only mutation must repair it from the block range of the part's own name,
-- `[min_block, max_block]`, which encloses the `_block_number` of every row of the part.

DROP TABLE IF EXISTS t_minmax_repair_merged;

CREATE TABLE t_minmax_repair_merged (date1 Date, value1 String, value2 UInt64) ENGINE = MergeTree ORDER BY tuple()
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1,
         part_minmax_index_columns = 'partition_key_only', min_bytes_for_wide_part = 0,
         min_bytes_for_full_part_storage = 0;

INSERT INTO t_minmax_repair_merged SELECT toDate('2018-10-01') + number % 3, toString(number), number FROM numbers(3);
INSERT INTO t_minmax_repair_merged SELECT toDate('2018-10-01') + number % 3, toString(number), number FROM numbers(3, 3);
INSERT INTO t_minmax_repair_merged SELECT toDate('2018-10-01') + number % 3, toString(number), number FROM numbers(6, 3);

-- The merged part covers blocks 1 to 3 and has no `minmax__block_number.idx`.
OPTIMIZE TABLE t_minmax_repair_merged FINAL;

ALTER TABLE t_minmax_repair_merged MODIFY SETTING part_minmax_index_columns = 'with_block_number_offset';

DETACH TABLE t_minmax_repair_merged SYNC;
ATTACH TABLE t_minmax_repair_merged;

-- The block ranges of the merged part are unknown - this is the state to be repaired.
SELECT '-- inherited from a part without the index --';
SELECT DISTINCT part_name, minmax__block_number, minmax__block_offset
FROM mergeTreeIndex(currentDatabase(), 't_minmax_repair_merged', with_minmax = 1) ORDER BY part_name;

-- A column-only mutation: the part is `Wide`, so the untouched columns are only hardlinked.
ALTER TABLE t_minmax_repair_merged UPDATE value1 = 'x' WHERE 1 SETTINGS mutations_sync = 2;

-- The repair must be visible to queries immediately, not only after the index is read back from disk.
SELECT '-- after the mutation, before a reload --';
SELECT DISTINCT part_name, minmax__block_number, minmax__block_offset
FROM mergeTreeIndex(currentDatabase(), 't_minmax_repair_merged', with_minmax = 1) ORDER BY part_name;

SELECT '-- pruning works without a reload --';
SELECT count() FROM t_minmax_repair_merged WHERE _block_number = 100 SETTINGS max_rows_to_read = 1;

DETACH TABLE t_minmax_repair_merged SYNC;
ATTACH TABLE t_minmax_repair_merged;

-- `_block_number` is repaired to the block range of the part's own name; `_block_offset` stays unknown,
-- because a mutation may have dropped rows and the row count of the original block is no longer recoverable.
SELECT '-- after the mutation and a reload --';
SELECT DISTINCT part_name, minmax__block_number, minmax__block_offset
FROM mergeTreeIndex(currentDatabase(), 't_minmax_repair_merged', with_minmax = 1) ORDER BY part_name;

-- The whole part is pruned away, so nothing is read at all.
SELECT '-- pruning works again --';
SELECT count() FROM t_minmax_repair_merged WHERE _block_number = 100 SETTINGS max_rows_to_read = 1;

-- The rows kept their per-row block numbers, and the repaired range still admits all of them.
SELECT '-- the repaired range admits every row --';
SELECT count() FROM t_minmax_repair_merged WHERE _block_number BETWEEN 1 AND 3;

DROP TABLE t_minmax_repair_merged;
