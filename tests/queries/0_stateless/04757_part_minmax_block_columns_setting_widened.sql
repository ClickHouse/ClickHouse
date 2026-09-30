-- Tags: no-shared-merge-tree
-- no-shared-merge-tree: RMT/SMT allocate block numbers starting from 0

-- Widening `part_minmax_index_columns` on a live table does not reload the parts already in memory, so a
-- part written under `partition_key_only` still carries a minmax index without the block column slots at
-- all. A mutation must grow the inherited index to the current set of minmax columns and repair the
-- block column ranges - otherwise the mutated part is written without the block column files and reads
-- back the whole universe after a reload.

DROP TABLE IF EXISTS t_minmax_widened;

CREATE TABLE t_minmax_widened (date1 Date, value1 String, value2 UInt64) ENGINE = MergeTree ORDER BY tuple()
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1,
         part_minmax_index_columns = 'partition_key_only', min_bytes_for_wide_part = 0,
         min_bytes_for_full_part_storage = 0;

INSERT INTO t_minmax_widened SELECT toDate('2018-10-01') + number % 3, toString(number), number FROM numbers(9);

-- Widen the setting while the part is loaded: its in-memory index has no block column slots.
ALTER TABLE t_minmax_widened MODIFY SETTING part_minmax_index_columns = 'with_block_number_offset';

-- A column-only mutation: the part is `Wide`, so the untouched columns are only hardlinked.
ALTER TABLE t_minmax_widened UPDATE value1 = 'x' WHERE 1 SETTINGS mutations_sync = 2;

-- The repair must be visible to queries immediately, not only after the index is read back from disk.
SELECT '-- after the mutation, before a reload --';
SELECT DISTINCT part_name, minmax__block_number, minmax__block_offset
FROM mergeTreeIndex(currentDatabase(), 't_minmax_widened', with_minmax = 1) ORDER BY part_name;

SELECT '-- pruning works without a reload --';
SELECT count() FROM t_minmax_widened WHERE _block_number = 100 SETTINGS max_rows_to_read = 1;
SELECT count() FROM t_minmax_widened WHERE _block_offset = 100 SETTINGS max_rows_to_read = 1;

DETACH TABLE t_minmax_widened SYNC;
ATTACH TABLE t_minmax_widened;

-- `_block_number` is repaired from the part's own block range. `_block_offset` is repaired too, because
-- the source part is a single never-mutated block, so its offsets are exactly `[0, rows_count - 1]` and
-- the column-only mutation keeps every row.
SELECT '-- after a reload --';
SELECT DISTINCT part_name, minmax__block_number, minmax__block_offset
FROM mergeTreeIndex(currentDatabase(), 't_minmax_widened', with_minmax = 1) ORDER BY part_name;

SELECT '-- pruning works again --';
SELECT count() FROM t_minmax_widened WHERE _block_number = 100 SETTINGS max_rows_to_read = 1;
SELECT count() FROM t_minmax_widened WHERE _block_offset = 100 SETTINGS max_rows_to_read = 1;

DROP TABLE t_minmax_widened;
