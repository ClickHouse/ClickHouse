-- Tags: no-replicated-database
-- no-replicated-database: 03100_lwu_03_join

SET insert_keeper_fault_injection_probability = 0.0;
SET enable_lightweight_update = 1;

DROP TABLE IF EXISTS t_shared SYNC;

CREATE TABLE t_shared (id UInt64, c1 UInt64, s String)
ENGINE = ReplicatedMergeTree('/zookeeper/{database}/t_shared/', '1')
ORDER BY id
SETTINGS
    enable_block_number_column = true,
    enable_block_offset_column = true,
    shared_merge_tree_disable_merges_and_mutations_assignment = 1,
    apply_patches_on_merge = 0,
    patch_parts_version = 'v2';

INSERT INTO t_shared SELECT number, number, 's' || toString(number) FROM numbers(20);

UPDATE t_shared SET c1 = c1 * 10 WHERE id % 2 = 0;
UPDATE t_shared SET s = s || '_foo' WHERE id % 2 = 1;
UPDATE t_shared SET c1 = c1 + 1000 WHERE id % 3 = 0;

OPTIMIZE TABLE t_shared FINAL SETTINGS optimize_throw_if_noop = 1;
OPTIMIZE TABLE t_shared PARTITION ID 'patch-fa2434eb4a6545e335e6f73a442431b7-all' FINAL SETTINGS optimize_throw_if_noop = 1;
OPTIMIZE TABLE t_shared PARTITION ID 'patch-217fed773f8b0f1741d2ccea2e599cdf-all' FINAL SETTINGS optimize_throw_if_noop = 1;

SET apply_patch_parts = 1;
SELECT * FROM t_shared ORDER BY id;

SELECT name, rows FROM system.parts WHERE database = currentDatabase() AND table = 't_shared' AND active ORDER BY name;

DROP TABLE t_shared SYNC;
