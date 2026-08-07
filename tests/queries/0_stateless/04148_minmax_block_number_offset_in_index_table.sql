-- Tags: no-shared-merge-tree, no-random-merge-tree-settings
-- Tag no-shared-merge-tree: RMT/SMT allocate block numbers starting from 0

DROP TABLE IF EXISTS t_mt;

-- Default `partition_key_only`: only partition columns are in the per-part minmax index.
CREATE TABLE t_mt (id UInt64, p UInt64) ENGINE = MergeTree
PARTITION BY p ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

SYSTEM STOP MERGES t_mt;

INSERT INTO t_mt SELECT number, 0 FROM numbers(5); -- 0_1_1_0
INSERT INTO t_mt SELECT number, 1 FROM numbers(7); -- 1_2_2_0

SELECT '-- partition_key_only: minmax_p per part --';
SELECT DISTINCT part_name, minmax_p
FROM mergeTreeIndex(currentDatabase(), 't_mt', with_minmax = 1)
ORDER BY part_name;

SELECT '-- partition_key_only: minmax__block_number is not exposed --';
SELECT minmax__block_number FROM mergeTreeIndex(currentDatabase(), 't_mt', with_minmax = 1) LIMIT 0; -- { serverError UNKNOWN_IDENTIFIER }

SELECT '-- partition_key_only: minmax__block_offset is not exposed --';
SELECT minmax__block_offset FROM mergeTreeIndex(currentDatabase(), 't_mt', with_minmax = 1) LIMIT 0; -- { serverError UNKNOWN_IDENTIFIER }

-- Switch to `with_block_number_offset`: persisted virtual columns are added to the per-part minmax index.
ALTER TABLE t_mt MODIFY SETTING part_minmax_index_columns = 'with_block_number_offset' SETTINGS alter_sync = 2;

INSERT INTO t_mt SELECT number, 0 FROM numbers(3); -- 0_3_3_0, block 3, offsets 0..2

SELECT '-- with_block_number_offset: minmax ranges for the new 0-level part --';
SELECT DISTINCT part_name, minmax_p, minmax__block_number, minmax__block_offset
FROM mergeTreeIndex(currentDatabase(), 't_mt', with_minmax = 1)
ORDER BY part_name;

DROP TABLE t_mt;
