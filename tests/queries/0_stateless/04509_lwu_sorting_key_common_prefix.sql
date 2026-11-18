-- Lightweight updates combined with ALTER MODIFY ORDER BY. v2 patch parts persist the
-- sort-key children they were written with; reads and merges apply the patch over the
-- longest common prefix of the persisted and the current sorting key.

SET enable_lightweight_update = 1;
SET apply_patch_parts = 1;

DROP TABLE IF EXISTS t_lwu_key_prefix SYNC;

-- Patch written before the key is extended still applies after; a second patch is written
-- with the extended key. Patches with different persisted keys apply to the same table.
CREATE TABLE t_lwu_key_prefix (a UInt64, v String)
ENGINE = MergeTree
PRIMARY KEY a
ORDER BY a
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, patch_parts_version = 'v2';

INSERT INTO t_lwu_key_prefix SELECT number, 'foo' FROM numbers(1000);
UPDATE t_lwu_key_prefix SET v = 'bar' WHERE a < 100;

ALTER TABLE t_lwu_key_prefix ADD COLUMN b UInt64, MODIFY ORDER BY (a, b);
UPDATE t_lwu_key_prefix SET v = 'baz' WHERE a >= 900;

SELECT count() FROM system.parts
WHERE database = currentDatabase() AND table = 't_lwu_key_prefix' AND active AND startsWith(name, 'patch');

SELECT countIf(v = 'bar'), countIf(v = 'baz'), countIf(v = 'foo') FROM t_lwu_key_prefix;

DROP TABLE t_lwu_key_prefix SYNC;

-- Patch application on merge materializes the update after the key was shrunk.
CREATE TABLE t_lwu_key_prefix (a UInt64, b UInt64, v String)
ENGINE = MergeTree
PRIMARY KEY a
ORDER BY (a, b)
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1,
         patch_parts_version = 'v2', apply_patches_on_merge = 1;

INSERT INTO t_lwu_key_prefix SELECT number, number % 10, 'foo' FROM numbers(1000);
UPDATE t_lwu_key_prefix SET v = 'bar' WHERE a < 100;

ALTER TABLE t_lwu_key_prefix MODIFY ORDER BY a;

OPTIMIZE TABLE t_lwu_key_prefix FINAL;
SELECT count() FROM t_lwu_key_prefix WHERE v = 'bar' SETTINGS apply_patch_parts = 0;

DROP TABLE t_lwu_key_prefix SYNC;

-- An equal-key run spanning several patch chunks and main read blocks after the key
-- was shrunk: the effective key (k) is non-unique for the whole table.
CREATE TABLE t_lwu_key_prefix (k UInt64, u UInt64, v UInt64)
ENGINE = MergeTree
PRIMARY KEY k
ORDER BY (k, u)
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1,
         patch_parts_version = 'v2', index_granularity = 8192;

INSERT INTO t_lwu_key_prefix SELECT 0, number, number FROM numbers(200000);
UPDATE t_lwu_key_prefix SET v = v + 10000000 WHERE 1;

ALTER TABLE t_lwu_key_prefix MODIFY ORDER BY k;

SELECT count() FROM t_lwu_key_prefix WHERE v >= 10000000
SETTINGS merge_tree_min_read_task_size = 8, max_block_size = 65409;

DROP TABLE t_lwu_key_prefix SYNC;
