-- Tags: no-shared-merge-tree, no-random-merge-tree-settings
-- Tag no-shared-merge-tree: RMT/SMT allocate block numbers starting from 0
-- Tag no-random-merge-tree-settings: the test toggles part_minmax_index_columns and needs
--   enable_block_number_column/enable_block_offset_column fixed to 1 (randomizing them breaks the setup)

-- Regression test for a LOGICAL_ERROR "Part level Min-Max index was constructed from unexpected
-- columns set" that used to abort the merge below in getProbablyWrittenFiles. The min-max file-order
-- hint (built by getPreferredFileOrder for compact parts) rejected an in-memory hyperrectangle that
-- was larger than the current part_minmax_index_columns setting, which happens after the setting is
-- lowered while a part still carries a cached with_block_number_offset index. The hint is consumed
-- by packed storage to lay out the archive, so the table uses packed compact parts (the CI case).

DROP TABLE IF EXISTS t;

-- min_bytes_for_wide_part = huge -> compact parts; min_bytes_for_full_part_storage = huge -> packed
-- storage, whose archive layout actually consumes the file-order hint returned by getProbablyWrittenFiles.
CREATE TABLE t (id UInt64, p UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1,
         part_minmax_index_columns = 'with_block_number_offset',
         min_bytes_for_wide_part = '1G', min_bytes_for_full_part_storage = '1G';

SYSTEM STOP MERGES t;

INSERT INTO t SELECT 1, 1;
INSERT INTO t SELECT 2, 1;
INSERT INTO t SELECT 3, 1;

-- Load and cache each part's minmax index at the with_block_number_offset size.
SELECT count() FROM t WHERE _block_number >= 0;
SELECT count() FROM t WHERE _block_offset >= 0;

-- Lower the setting; the cached in-memory index keeps the larger column set.
ALTER TABLE t MODIFY SETTING part_minmax_index_columns = 'partition_key_only' SETTINGS alter_sync = 2;

SYSTEM START MERGES t;

-- The merge used to abort with a LOGICAL_ERROR here.
OPTIMIZE TABLE t FINAL;

-- The merge must have completed into a single active packed compact part.
SELECT count(), any(part_type), any(part_storage_type) FROM system.parts WHERE database = currentDatabase() AND table = 't' AND active;
-- Reading back proves the packed archive (laid out using the hint) is intact.
SELECT count() FROM t;
SELECT id FROM t WHERE _block_number >= 0 ORDER BY id;

DROP TABLE t;
