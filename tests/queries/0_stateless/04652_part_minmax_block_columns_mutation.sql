-- Tags: no-shared-merge-tree
-- Tag no-shared-merge-tree: RMT/SMT allocate block numbers starting from 0

-- Mutations that rewrite a part must materialize `_block_number` / `_block_offset`
-- so the per-part minmax index over them can be rebuilt.

DROP TABLE IF EXISTS t_mut;

CREATE TABLE t_mut (date1 Date, value1 String, value2 UInt64) ENGINE = MergeTree ORDER BY tuple()
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1,
         part_minmax_index_columns = 'with_block_number_offset';

INSERT INTO t_mut SELECT toDate('2018-10-01') + number % 3, toString(number), number FROM numbers(9);

SELECT '-- after insert --';
SELECT DISTINCT part_name, minmax__block_number, minmax__block_offset
FROM mergeTreeIndex(currentDatabase(), 't_mut', with_minmax = 1) ORDER BY part_name;

SELECT '-- ALTER UPDATE --';
ALTER TABLE t_mut UPDATE value1 = 'x' WHERE 1 SETTINGS mutations_sync = 2;
SELECT DISTINCT part_name, minmax__block_number, minmax__block_offset
FROM mergeTreeIndex(currentDatabase(), 't_mut', with_minmax = 1) ORDER BY part_name;

SELECT '-- ALTER RENAME COLUMN --';
ALTER TABLE t_mut RENAME COLUMN date1 TO renamed_date1 SETTINGS mutations_sync = 2;
SELECT DISTINCT part_name, minmax__block_number, minmax__block_offset
FROM mergeTreeIndex(currentDatabase(), 't_mut', with_minmax = 1) ORDER BY part_name;

SELECT '-- ALTER DROP COLUMN --';
ALTER TABLE t_mut DROP COLUMN value2 SETTINGS mutations_sync = 2;
SELECT DISTINCT part_name, minmax__block_number, minmax__block_offset
FROM mergeTreeIndex(currentDatabase(), 't_mut', with_minmax = 1) ORDER BY part_name;

-- Deleting the rows at offsets 0, 3 and 6 narrows the `_block_offset` range.
SELECT '-- ALTER DELETE --';
ALTER TABLE t_mut DELETE WHERE renamed_date1 = toDate('2018-10-01') SETTINGS mutations_sync = 2;
SELECT DISTINCT part_name, minmax__block_number, minmax__block_offset
FROM mergeTreeIndex(currentDatabase(), 't_mut', with_minmax = 1) ORDER BY part_name;

SELECT '-- surviving rows --';
SELECT count(), min(_block_number), max(_block_number), min(_block_offset), max(_block_offset) FROM t_mut;

DROP TABLE t_mut;
