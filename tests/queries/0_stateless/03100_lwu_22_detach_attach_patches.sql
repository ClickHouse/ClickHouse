-- Tags: no-replicated-database
-- no-replicated-database: fails due to additional shard.

DROP TABLE IF EXISTS t_detach_attach_patches SYNC;
DROP TABLE IF EXISTS t_detach_attach_patches_dst SYNC;

SET enable_lightweight_update = 1;

CREATE TABLE t_detach_attach_patches (id UInt64, a UInt64, b UInt64, c UInt64)
ENGINE = ReplicatedMergeTree('/zookeeper/{database}/t_lwu_on_fly/', '1')
ORDER BY a PARTITION BY id
SETTINGS
    enable_block_number_column = 1,
    enable_block_offset_column = 1;

CREATE TABLE t_detach_attach_patches_dst AS t_detach_attach_patches
ENGINE = ReplicatedMergeTree('/zookeeper/{database}/t_lwu_on_fly_dst/', '1')
ORDER BY a PARTITION BY id
SETTINGS
    enable_block_number_column = 1,
    enable_block_offset_column = 1;

SET apply_patch_parts = 1;
SET mutations_sync = 2;
SET insert_keeper_fault_injection_probability = 0.0;

INSERT INTO t_detach_attach_patches VALUES (0, 1, 1, 1) (0, 2, 2, 2);
INSERT INTO t_detach_attach_patches VALUES (1, 1, 1, 1) (1, 2, 2, 2);
INSERT INTO t_detach_attach_patches VALUES (2, 1, 1, 1) (2, 2, 2, 2);
INSERT INTO t_detach_attach_patches VALUES (3, 1, 1, 1) (3, 2, 2, 2);
INSERT INTO t_detach_attach_patches VALUES (4, 1, 1, 1) (4, 2, 2, 2);
INSERT INTO t_detach_attach_patches VALUES (5, 1, 1, 1) (5, 2, 2, 2);

UPDATE t_detach_attach_patches SET b = b + 1 WHERE a = 1;
UPDATE t_detach_attach_patches SET c = c + 2 WHERE a = 2;
UPDATE t_detach_attach_patches SET b = b + 3, c = c + 3 WHERE 1;

SELECT '==========';
SELECT * FROM t_detach_attach_patches ORDER BY ALL;

ALTER TABLE t_detach_attach_patches DETACH PARTITION 0; -- { serverError SUPPORT_IS_DISABLED }
ALTER TABLE t_detach_attach_patches APPLY PATCHES IN PARTITION 0;
ALTER TABLE t_detach_attach_patches DETACH PARTITION 0;

ALTER TABLE t_detach_attach_patches DETACH PART '1_0_0_0'; -- { serverError SUPPORT_IS_DISABLED }
ALTER TABLE t_detach_attach_patches APPLY PATCHES IN PARTITION 1;
ALTER TABLE t_detach_attach_patches DETACH PART '1_0_0_0_4';

ALTER TABLE t_detach_attach_patches MOVE PARTITION 2 TO TABLE t_detach_attach_patches_dst; -- { serverError SUPPORT_IS_DISABLED }
ALTER TABLE t_detach_attach_patches APPLY PATCHES IN PARTITION 2;
ALTER TABLE t_detach_attach_patches MOVE PARTITION 2 TO TABLE t_detach_attach_patches_dst;

ALTER TABLE t_detach_attach_patches_dst REPLACE PARTITION 3 FROM t_detach_attach_patches; -- { serverError SUPPORT_IS_DISABLED }
ALTER TABLE t_detach_attach_patches APPLY PATCHES IN PARTITION 3;
ALTER TABLE t_detach_attach_patches_dst REPLACE PARTITION 3 FROM t_detach_attach_patches;

ALTER TABLE t_detach_attach_patches DROP PARTITION 4;
ALTER TABLE t_detach_attach_patches DROP PART '5_0_0_0';

SELECT '==========';
SELECT * FROM t_detach_attach_patches ORDER BY ALL;

ALTER TABLE t_detach_attach_patches ATTACH PARTITION 0;
ALTER TABLE t_detach_attach_patches ATTACH PART '1_0_0_0_4';

SET apply_patch_parts = 0;

SELECT '==========';
SELECT * FROM t_detach_attach_patches ORDER BY ALL;

DROP TABLE t_detach_attach_patches SYNC;
DROP TABLE t_detach_attach_patches_dst SYNC;
