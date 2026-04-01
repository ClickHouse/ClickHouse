-- Tags: no-replicated-database, long
-- Tag no-replicated-database: profile events for mutations may differ because of additional replicas.

DROP TABLE IF EXISTS t_apply_patches SYNC;
DROP TABLE IF EXISTS t_apply_patches_smt SYNC;

SET enable_lightweight_update = 1;

CREATE TABLE t_apply_patches (a UInt64, b UInt64, c UInt64, d UInt64)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS
    min_bytes_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0,
    ratio_of_defaults_for_sparse_serialization = 1.0,
    enable_block_number_column = 1,
    enable_block_offset_column = 1;

SET mutations_sync = 2;

INSERT INTO t_apply_patches SELECT number, 0, 0, 0 FROM numbers(10000);

UPDATE t_apply_patches SET b = 1 WHERE a % 4 = 0;
UPDATE t_apply_patches SET c = 2 WHERE a % 4 = 0;
UPDATE t_apply_patches SET b = 3, c = 4 WHERE a % 4 = 1;

SELECT b, c, count() FROM t_apply_patches GROUP BY b, c ORDER BY b, c;

ALTER TABLE t_apply_patches APPLY PATCHES;

SELECT b, c, count() FROM t_apply_patches GROUP BY b, c ORDER BY b, c SETTINGS apply_patch_parts = 0;

SYSTEM FLUSH LOGS part_log;

SELECT
    ProfileEvents['MutationSomePartColumns'],
    ProfileEvents['MutatedUncompressedBytes'] -- 2 * 8 * 10000 = 160000, because only 2 columns must be affected.
FROM system.part_log WHERE database = currentDatabase() AND table = 't_apply_patches' AND event_type = 'MutatePart'
ORDER BY ALL;

CREATE TABLE t_apply_patches_smt (a UInt64, b UInt64, c UInt64, d UInt64)
ENGINE = ReplicatedMergeTree('/zookeeper/{database}/t_apply_patches_smt/', '1')
ORDER BY tuple()
SETTINGS
    min_bytes_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0,
    ratio_of_defaults_for_sparse_serialization = 1.0,
    enable_block_number_column = 1,
    enable_block_offset_column = 1;

SET mutations_sync = 2;

INSERT INTO t_apply_patches_smt SELECT number, 0, 0, 0 FROM numbers(10000);

UPDATE t_apply_patches_smt SET b = 1 WHERE a % 4 = 0;
UPDATE t_apply_patches_smt SET c = 2 WHERE a % 4 = 0;
UPDATE t_apply_patches_smt SET b = 3, c = 4 WHERE a % 4 = 1;

SELECT b, c, count() FROM t_apply_patches_smt GROUP BY b, c ORDER BY b, c;

ALTER TABLE t_apply_patches_smt APPLY PATCHES;

SELECT b, c, count() FROM t_apply_patches GROUP BY b, c ORDER BY b, c SETTINGS apply_patch_parts = 0;

SYSTEM FLUSH LOGS part_log;

SELECT
    ProfileEvents['MutationSomePartColumns'],
    ProfileEvents['MutatedUncompressedBytes'] -- 2 * 8 * 10000 = 160000, because only 2 columns must be affected.
FROM system.part_log WHERE database = currentDatabase() AND table = 't_apply_patches_smt' AND event_type = 'MutatePart'
ORDER BY ALL;

DROP TABLE IF EXISTS t_apply_patches SYNC;
DROP TABLE IF EXISTS t_apply_patches_smt SYNC;
