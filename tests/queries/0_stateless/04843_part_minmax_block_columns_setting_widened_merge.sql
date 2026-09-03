-- Tags: no-shared-merge-tree
-- no-shared-merge-tree: RMT/SMT allocate block numbers starting from 0

-- Like 04757_part_minmax_block_columns_setting_widened, but for an ordinary merge instead of a
-- column-only mutation: widening `part_minmax_index_columns` on a live table does not reload the parts
-- already in memory, so the source parts still carry a minmax index without the block column slots at
-- all. A non-row-reducing merge seeds the merged part's index by merging the source parts' indexes, and
-- `merge` truncates to the shorter of the two indexes - so an unrepaired narrow source would strip the
-- block columns from the merged index, the merged part would be stored without the block column files,
-- and it would read back the whole universe after a reload. The merge must repair each source index
-- (grow it to the current set of minmax columns and re-derive the block column ranges) before merging.

DROP TABLE IF EXISTS t_minmax_widened_merge;

CREATE TABLE t_minmax_widened_merge (date1 Date, value1 String, value2 UInt64) ENGINE = MergeTree ORDER BY tuple()
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1,
         part_minmax_index_columns = 'partition_key_only', min_bytes_for_wide_part = 0,
         min_bytes_for_full_part_storage = 0;

INSERT INTO t_minmax_widened_merge SELECT toDate('2018-10-01') + number % 3, toString(number), number FROM numbers(3);
INSERT INTO t_minmax_widened_merge SELECT toDate('2018-10-01') + number % 3, toString(number), number FROM numbers(3, 3);
INSERT INTO t_minmax_widened_merge SELECT toDate('2018-10-01') + number % 3, toString(number), number FROM numbers(6, 3);

-- Widen the setting while the parts are loaded: their in-memory indexes have no block column slots.
ALTER TABLE t_minmax_widened_merge MODIFY SETTING part_minmax_index_columns = 'with_block_number_offset';

-- An ordinary merge of the three loaded parts.
OPTIMIZE TABLE t_minmax_widened_merge FINAL;

-- The repair must be visible to queries immediately, not only after the index is read back from disk.
-- `_block_number` is the union of the source parts' block ranges; `_block_offset` is the union of
-- `[0, rows_count - 1]` of the sources, each of which is a single never-mutated block.
SELECT '-- after the merge, before a reload --';
SELECT DISTINCT part_name, minmax__block_number, minmax__block_offset
FROM mergeTreeIndex(currentDatabase(), 't_minmax_widened_merge', with_minmax = 1) WHERE part_name = 'all_1_3_1' ORDER BY part_name;

SELECT '-- pruning works without a reload --';
SELECT count() FROM t_minmax_widened_merge WHERE _block_number = 100 SETTINGS max_rows_to_read = 1;
SELECT count() FROM t_minmax_widened_merge WHERE _block_offset = 100 SETTINGS max_rows_to_read = 1;

DETACH TABLE t_minmax_widened_merge SYNC;
ATTACH TABLE t_minmax_widened_merge;

SELECT '-- after a reload --';
SELECT DISTINCT part_name, minmax__block_number, minmax__block_offset
FROM mergeTreeIndex(currentDatabase(), 't_minmax_widened_merge', with_minmax = 1) WHERE part_name = 'all_1_3_1' ORDER BY part_name;

SELECT '-- pruning works again --';
SELECT count() FROM t_minmax_widened_merge WHERE _block_number = 100 SETTINGS max_rows_to_read = 1;
SELECT count() FROM t_minmax_widened_merge WHERE _block_offset = 100 SETTINGS max_rows_to_read = 1;

-- The rows kept their per-row block numbers, and the repaired range still admits all of them.
SELECT '-- the repaired range admits every row --';
SELECT count() FROM t_minmax_widened_merge WHERE _block_number BETWEEN 1 AND 3;

DROP TABLE t_minmax_widened_merge;
