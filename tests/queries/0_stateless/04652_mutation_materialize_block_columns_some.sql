-- Tags: no-random-settings, no-random-merge-tree-settings, no-shared-merge-tree

-- A lightweight UPDATE mutation materializes `_block_number` / `_block_offset` in parts
-- created before the columns were enabled (MutateSomePartColumns path, not REWRITE PARTS).

DROP TABLE IF EXISTS t_mut_block_columns;

-- Pin the two legacy level-0 parts against background merges: max_bytes_to_merge_at_max_space_in_pool = 1
-- caps the merge selector so no two parts are ever merged, which keeps the all_1_1_0_3 / all_2_2_0_3
-- part set stable and prevents the block columns from being materialized through the merge path.
-- SYSTEM STOP MERGES is unusable here: StorageMergeTree::selectPartsToMutate gates on the same
-- merges_blocker, so stopping merges would also block the UPDATE mutation this test needs.
CREATE TABLE t_mut_block_columns (id UInt32, a UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 0, enable_block_offset_column = 0, min_bytes_for_wide_part = 1,
         max_bytes_to_merge_at_max_space_in_pool = 1;

INSERT INTO t_mut_block_columns VALUES (1, 1), (2, 2), (3, 3);
INSERT INTO t_mut_block_columns VALUES (4, 4), (5, 5), (6, 6);

ALTER TABLE t_mut_block_columns MODIFY SETTING enable_block_number_column = 1, enable_block_offset_column = 1;

SET mutations_sync = 1;
ALTER TABLE t_mut_block_columns UPDATE a = a + 100 WHERE 1;

-- Both mutated parts now physically store the block columns.
SELECT name, column FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_mut_block_columns' AND active
  AND column IN ('_block_number', '_block_offset')
ORDER BY name, column;

SELECT id, a, _block_number, _block_offset FROM t_mut_block_columns ORDER BY id;

DROP TABLE t_mut_block_columns;
