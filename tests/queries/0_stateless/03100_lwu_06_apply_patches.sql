-- Tags: no-replicated-database
-- no-replicated-database: OPTIMIZE is replicated which affects the part names.

SET insert_keeper_fault_injection_probability = 0.0;
SET enable_lightweight_update = 1;

DROP TABLE IF EXISTS t_shared SYNC;

CREATE TABLE t_shared (id UInt64, c1 UInt64)
ENGINE = ReplicatedMergeTree('/zookeeper/{database}/t_shared/', '1')
ORDER BY id
SETTINGS
    enable_block_number_column = 1,
    enable_block_offset_column = 1,
    apply_patches_on_merge = 0,
    cleanup_delay_period = 1000,
    max_cleanup_delay_period = 1000;

INSERT INTO t_shared SELECT number, number FROM numbers(20);
INSERT INTO t_shared SELECT number, number FROM numbers(20, 10);

UPDATE t_shared SET c1 = id + 100 WHERE id % 2 = 0;

SET mutations_sync = 2;
ALTER TABLE t_shared APPLY PATCHES, UPDATE c1 = 2000 WHERE id % 10 = 0;

UPDATE t_shared SET c1 = id + 1000 WHERE id % 3 = 0;

SELECT '*** with patches ***';
SELECT * FROM t_shared ORDER BY id SETTINGS apply_patch_parts = 1;
SELECT '*** without patches ***';
SELECT * FROM t_shared ORDER BY id SETTINGS apply_patch_parts = 0;

SELECT name, rows FROM system.parts WHERE database = currentDatabase() AND table = 't_shared' AND active ORDER BY name;

SELECT * FROM t_shared ORDER BY id SETTINGS apply_patch_parts = 1;
SELECT name, rows FROM system.parts WHERE database = currentDatabase() AND table = 't_shared' AND active ORDER BY name;

OPTIMIZE TABLE t_shared PARTITION ID 'all' FINAL;

SELECT * FROM t_shared ORDER BY id SETTINGS apply_patch_parts = 1;
SELECT name, rows FROM system.parts WHERE database = currentDatabase() AND table = 't_shared' AND active ORDER BY name;

DROP TABLE t_shared SYNC;
