DROP TABLE IF EXISTS t_lwu_merges SYNC;
SET enable_lightweight_update = 1;

CREATE TABLE t_lwu_merges (id UInt64, u UInt64, s String)
ENGINE = ReplicatedMergeTree('/zookeeper/{database}/t_lwu_merges/', '1')
ORDER BY id PARTITION BY id % 2
SETTINGS
    enable_block_number_column = 1,
    enable_block_offset_column = 1,
    apply_patches_on_merge = 1,
    shared_merge_tree_disable_merges_and_mutations_assignment = 1,
    max_replicated_mutations_in_queue = 0;

INSERT INTO t_lwu_merges SELECT number, number, 'c' || number FROM numbers(10000);

UPDATE t_lwu_merges SET s = s || '_foo' WHERE id % 3 = 0;
UPDATE t_lwu_merges SET u = id * 10 WHERE id % 3 = 1;

SYSTEM SYNC REPLICA t_lwu_merges;

OPTIMIZE TABLE t_lwu_merges PARTITION 0 FINAL SETTINGS optimize_throw_if_noop = 1;

SELECT sum(u), countIf(endsWith(s, '_foo')) FROM t_lwu_merges SETTINGS apply_patch_parts = 0;
SELECT sum(u), countIf(endsWith(s, '_foo')) FROM t_lwu_merges SETTINGS apply_patch_parts = 1;

OPTIMIZE TABLE t_lwu_merges PARTITION 1 FINAL SETTINGS optimize_throw_if_noop = 1;

SELECT sum(u), countIf(endsWith(s, '_foo')) FROM t_lwu_merges SETTINGS apply_patch_parts = 0;
SELECT sum(u), countIf(endsWith(s, '_foo')) FROM t_lwu_merges SETTINGS apply_patch_parts = 1;

UPDATE t_lwu_merges SET u = 0 WHERE id % 3 = 1;
-- Add a barrier mutation between patch parts.
ALTER TABLE t_lwu_merges DELETE WHERE id = 0 SETTINGS mutations_sync = 0;
-- The second patch shouldn't be applied on merge until mutation is done.
UPDATE t_lwu_merges SET u = 0 WHERE id % 3 = 2;

SYSTEM SYNC REPLICA t_lwu_merges;

OPTIMIZE TABLE t_lwu_merges PARTITION 0 FINAL SETTINGS optimize_throw_if_noop = 1;
OPTIMIZE TABLE t_lwu_merges PARTITION 1 FINAL SETTINGS optimize_throw_if_noop = 1;

SELECT sum(u), countIf(endsWith(s, '_foo')) FROM t_lwu_merges SETTINGS apply_patch_parts = 0;
SELECT sum(u), countIf(endsWith(s, '_foo')) FROM t_lwu_merges SETTINGS apply_patch_parts = 1;

DROP TABLE t_lwu_merges SYNC;
