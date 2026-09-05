-- Test that the legacy format of patch parts (`patch_parts_version = 'v1'`) still works:
-- creation of patch parts, applying them on SELECT and merging them among themselves.
-- The partition id in OPTIMIZE is deterministic for v1 patch parts updating column `c1`.

DROP TABLE IF EXISTS t_lwu_merge_patches_v1 SYNC;
SET enable_lightweight_update = 1;

CREATE TABLE t_lwu_merge_patches_v1 (id UInt64, c1 UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, patch_parts_version = 'v1',
         -- the patch parts are listed one by one below, so only the explicit OPTIMIZE FINAL
         -- (which ignores this limit) may merge them
         max_bytes_to_merge_at_max_space_in_pool = 1;

INSERT INTO t_lwu_merge_patches_v1 SELECT number, number FROM numbers(20);

UPDATE t_lwu_merge_patches_v1 SET c1 = c1 + 100 WHERE id % 2 = 0;
UPDATE t_lwu_merge_patches_v1 SET c1 = c1 + 1000 WHERE id % 3 = 0;
UPDATE t_lwu_merge_patches_v1 SET c1 = 10000 WHERE id = 10;
UPDATE t_lwu_merge_patches_v1 SET c1 = 13000 WHERE id = 10;
UPDATE t_lwu_merge_patches_v1 SET c1 = 15000 WHERE id = 15;

SELECT * FROM t_lwu_merge_patches_v1 ORDER BY id SETTINGS apply_patch_parts = 1;
SELECT name, rows FROM system.parts WHERE database = currentDatabase() AND table = 't_lwu_merge_patches_v1' AND active ORDER BY min_block_number;

OPTIMIZE TABLE t_lwu_merge_patches_v1 PARTITION ID 'patch-63f56de952edf6cfcaf3d77635ceee5f-all' FINAL;

SELECT * FROM t_lwu_merge_patches_v1 ORDER BY id SETTINGS apply_patch_parts = 1;
SELECT name, rows FROM system.parts WHERE database = currentDatabase() AND table = 't_lwu_merge_patches_v1' AND active ORDER BY min_block_number;
SELECT count() FROM t_lwu_merge_patches_v1 WHERE c1 != id;

DROP TABLE t_lwu_merge_patches_v1 SYNC;
