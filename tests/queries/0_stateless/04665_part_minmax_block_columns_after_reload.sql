-- Tags: no-shared-merge-tree
-- no-shared-merge-tree: RMT/SMT allocate block numbers starting from 0

-- A mutation that does not rewrite the whole part must still materialize the per-part minmax index over
-- `_block_number` / `_block_offset`: the source part's ranges are synthesized at load time only while it is
-- a level-0, unmutated part, so without a file of its own the mutated part loses them on the next reload.

DROP TABLE IF EXISTS t_mut_reload;

CREATE TABLE t_mut_reload (date1 Date, value1 String, value2 UInt64) ENGINE = MergeTree ORDER BY tuple()
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1,
         part_minmax_index_columns = 'with_block_number_offset', min_bytes_for_wide_part = 0,
         min_bytes_for_full_part_storage = 0;

INSERT INTO t_mut_reload SELECT toDate('2018-10-01') + number % 3, toString(number), number FROM numbers(9);

ALTER TABLE t_mut_reload UPDATE value1 = 'x' WHERE 1 SETTINGS mutations_sync = 2;
ALTER TABLE t_mut_reload RENAME COLUMN date1 TO renamed_date1 SETTINGS mutations_sync = 2;
ALTER TABLE t_mut_reload DROP COLUMN value2 SETTINGS mutations_sync = 2;

SELECT '-- before reload --';
SELECT DISTINCT part_name, minmax__block_number, minmax__block_offset
FROM mergeTreeIndex(currentDatabase(), 't_mut_reload', with_minmax = 1) ORDER BY part_name;

DETACH TABLE t_mut_reload SYNC;
ATTACH TABLE t_mut_reload;

SELECT '-- after reload --';
SELECT DISTINCT part_name, minmax__block_number, minmax__block_offset
FROM mergeTreeIndex(currentDatabase(), 't_mut_reload', with_minmax = 1) ORDER BY part_name;

-- Deleting the rows at offsets 0, 3 and 6 narrows the `_block_offset` range, and the narrowed range must
-- also survive a reload.
ALTER TABLE t_mut_reload DELETE WHERE renamed_date1 = toDate('2018-10-01') SETTINGS mutations_sync = 2;

DETACH TABLE t_mut_reload SYNC;
ATTACH TABLE t_mut_reload;

SELECT '-- after delete and reload --';
SELECT DISTINCT part_name, minmax__block_number, minmax__block_offset
FROM mergeTreeIndex(currentDatabase(), 't_mut_reload', with_minmax = 1) ORDER BY part_name;

DROP TABLE t_mut_reload;
