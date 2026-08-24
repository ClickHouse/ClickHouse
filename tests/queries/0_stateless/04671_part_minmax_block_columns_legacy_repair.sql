-- Tags: no-shared-merge-tree
-- no-shared-merge-tree: RMT/SMT allocate block numbers starting from 0

-- A part that was mutated while `part_minmax_index_columns` did not cover `_block_number` has no
-- `minmax__block_number.idx` of its own, and `MinMaxIndex::load` may not synthesize the range for a mutated
-- part, so the range comes back as the whole universe. A later mutation must repair it instead of carrying
-- the lost range forward until a merge or a full rewrite happens. The same shape is what a table upgraded
-- from a server that did not materialize the index for mutated parts looks like.

DROP TABLE IF EXISTS t_minmax_repair;

CREATE TABLE t_minmax_repair (date1 Date, value1 String, value2 UInt64) ENGINE = MergeTree ORDER BY tuple()
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1,
         part_minmax_index_columns = 'partition_key_only', min_bytes_for_wide_part = 0,
         min_bytes_for_full_part_storage = 0;

INSERT INTO t_minmax_repair SELECT toDate('2018-10-01') + number % 3, toString(number), number FROM numbers(9);

-- A column-only mutation: the part is `Wide`, so the untouched columns are only hardlinked.
ALTER TABLE t_minmax_repair UPDATE value1 = 'x' WHERE 1 SETTINGS mutations_sync = 2;

ALTER TABLE t_minmax_repair MODIFY SETTING part_minmax_index_columns = 'with_block_number_offset';

DETACH TABLE t_minmax_repair SYNC;
ATTACH TABLE t_minmax_repair;

-- The block ranges of the already mutated part are unknown - this is the state to be repaired.
SELECT '-- inherited from a part without the index --';
SELECT DISTINCT part_name, minmax__block_number, minmax__block_offset
FROM mergeTreeIndex(currentDatabase(), 't_minmax_repair', with_minmax = 1) ORDER BY part_name;

ALTER TABLE t_minmax_repair UPDATE value2 = value2 + 1 WHERE 1 SETTINGS mutations_sync = 2;

-- The repair must be visible to queries immediately, not only after the index is read back from disk.
SELECT '-- after the next mutation, before a reload --';
SELECT DISTINCT part_name, minmax__block_number, minmax__block_offset
FROM mergeTreeIndex(currentDatabase(), 't_minmax_repair', with_minmax = 1) ORDER BY part_name;

SELECT '-- pruning works without a reload --';
SELECT count() FROM t_minmax_repair WHERE _block_number = 100 SETTINGS max_rows_to_read = 1;

DETACH TABLE t_minmax_repair SYNC;
ATTACH TABLE t_minmax_repair;

-- `_block_number` is repaired from the part's own block range; `_block_offset` stays unknown, because a
-- mutation may have dropped rows and the row count of the original block is no longer recoverable.
SELECT '-- after the next mutation and a reload --';
SELECT DISTINCT part_name, minmax__block_number, minmax__block_offset
FROM mergeTreeIndex(currentDatabase(), 't_minmax_repair', with_minmax = 1) ORDER BY part_name;

-- The whole part is pruned away, so nothing is read at all.
SELECT '-- pruning works again --';
SELECT count() FROM t_minmax_repair WHERE _block_number = 100 SETTINGS max_rows_to_read = 1;

DROP TABLE t_minmax_repair;
