-- Regression test: lost updates when an equal-sort-key run spans several patch read
-- chunks. The patch reader must keep reading patch ranges on sort-key equality,
-- because sort keys are not unique and the run may continue in the next chunk.

DROP TABLE IF EXISTS t_lwu_dup_keys;

CREATE TABLE t_lwu_dup_keys (k UInt64, v UInt64)
ENGINE = MergeTree ORDER BY k
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1,
         patch_parts_version = 'v2', index_granularity = 8192;

-- Single equal-key run longer than one main read block and several patch chunks.
INSERT INTO t_lwu_dup_keys SELECT 0, number FROM numbers(300000);
UPDATE t_lwu_dup_keys SET v = v + 10000000 WHERE 1;

SELECT count() FROM t_lwu_dup_keys WHERE v >= 10000000
SETTINGS merge_tree_min_read_task_size = 8, max_block_size = 65409;

DROP TABLE t_lwu_dup_keys;

-- Runs of 100000 rows per key: each run spans multiple main read blocks
-- and multiple patch chunks.
CREATE TABLE t_lwu_dup_keys (k UInt64, v UInt64)
ENGINE = MergeTree ORDER BY k
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1,
         patch_parts_version = 'v2', index_granularity = 8192;

INSERT INTO t_lwu_dup_keys SELECT intDiv(number, 100000), number FROM numbers(300000);
UPDATE t_lwu_dup_keys SET v = v + 10000000 WHERE 1;

SELECT count() FROM t_lwu_dup_keys WHERE v >= 10000000
SETTINGS merge_tree_min_read_task_size = 8, max_block_size = 65409;

DROP TABLE t_lwu_dup_keys;
