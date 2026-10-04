-- Tags: no-random-settings, no-random-merge-tree-settings, no-parallel-replicas

-- The implicit min-max skip index over the persistent virtual columns _block_number / _block_offset
-- must not prevent column ALTERs (RENAME / ADD / MODIFY) on an unrelated column.
-- Before the fix, ALTER validation re-resolved the implicit index against physical columns only,
-- so _block_number could not be resolved and the ALTER threw UNKNOWN_IDENTIFIER.

SET enable_analyzer = 1;
SET use_skip_indexes = 1;

DROP TABLE IF EXISTS t_imv_alter;
CREATE TABLE t_imv_alter (id UInt64, s String)
ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1,
         add_minmax_index_for_block_number_column = 1, add_minmax_index_for_block_offset_column = 1,
         index_granularity = 1;

SYSTEM STOP MERGES t_imv_alter;
INSERT INTO t_imv_alter SELECT number, toString(number) FROM numbers(10);
INSERT INTO t_imv_alter SELECT number + 10, toString(number) FROM numbers(10);
SYSTEM START MERGES t_imv_alter;
OPTIMIZE TABLE t_imv_alter FINAL;

ALTER TABLE t_imv_alter RENAME COLUMN s TO s2;
ALTER TABLE t_imv_alter ADD COLUMN extra Int32;
ALTER TABLE t_imv_alter MODIFY COLUMN id UInt64 CODEC(ZSTD);

SELECT name, expr FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't_imv_alter' ORDER BY name;

-- The implicit indices must still PRUNE granules after the ALTERs, not merely stay attached.
-- EXPLAIN indexes = 1 prints the index name even when it reads every granule (Granules: N/N),
-- so a name match alone would pass a regression that stopped pruning. Assert on the Granules line.
-- 20 rows in two blocks, index_granularity = 1: _block_number = 1 matches one block (10/20 granules read),
-- _block_offset = 0 matches one row per block (2/20). A stopped-pruning regression reads 20/20 and fails these.
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT * FROM t_imv_alter WHERE _block_number = 1) WHERE explain ILIKE '%Granules: 10/20%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT * FROM t_imv_alter WHERE _block_offset = 0) WHERE explain ILIKE '%Granules: 2/20%';
SELECT count() FROM t_imv_alter;

DROP TABLE IF EXISTS t_imv_alter;
