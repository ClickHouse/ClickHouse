-- Test that patch parts of different format versions (v1 and v2) can coexist in one table:
-- both are applied on SELECT and materialized on merge. This is the state left behind
-- by lightweight updates in a cluster with mixed values of `patch_parts_version`.

DROP TABLE IF EXISTS t_lwu_mixed_versions SYNC;
SET enable_lightweight_update = 1;

CREATE TABLE t_lwu_mixed_versions (id UInt64, s String, v UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, apply_patches_on_merge = 1, patch_parts_version = 'v1';

INSERT INTO t_lwu_mixed_versions SELECT number, 'foo', 0 FROM numbers(10);

SYSTEM STOP MERGES t_lwu_mixed_versions;

UPDATE t_lwu_mixed_versions SET v = 1 WHERE id % 2 = 0;

ALTER TABLE t_lwu_mixed_versions MODIFY SETTING patch_parts_version = 'v2';

UPDATE t_lwu_mixed_versions SET v = 2 WHERE id >= 5;
UPDATE t_lwu_mixed_versions SET s = 'bar' WHERE id = 3;

-- v1 and v2 patch parts must belong to different partitions even for the same set of updated columns.
SELECT count(DISTINCT partition_id), count() FROM system.parts
WHERE database = currentDatabase() AND table = 't_lwu_mixed_versions' AND active AND startsWith(partition_id, 'patch-');

SELECT id, s, v FROM t_lwu_mixed_versions ORDER BY id SETTINGS apply_patch_parts = 1;

SYSTEM START MERGES t_lwu_mixed_versions;
OPTIMIZE TABLE t_lwu_mixed_versions FINAL;

SELECT id, s, v FROM t_lwu_mixed_versions ORDER BY id SETTINGS apply_patch_parts = 0;

DROP TABLE t_lwu_mixed_versions SYNC;
