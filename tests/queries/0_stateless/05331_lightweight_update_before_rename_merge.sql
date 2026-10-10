-- Tags: zookeeper, no-replicated-database, no-shared-merge-tree
-- no-replicated-database: relies on alter_sync = 0 and SYSTEM SYNC REPLICA ordering of a single replica
-- no-shared-merge-tree: relies on max_replicated_mutations_in_queue
-- A lightweight UPDATE made before RENAME COLUMN must survive a merge that writes the part at the new metadata version.

SET enable_lightweight_update = 1;
SET optimize_throw_if_noop = 1;

-- A mutation queued behind an unfinished ALTER mutation is assigned only by the next merge-selecting round,
-- whose interval otherwise backs off up to max_merge_selecting_sleep_ms (60 s).

-- Updates before and after the rename, then a merge that does not apply patches.
DROP TABLE IF EXISTS t_lwu_rename SYNC;
CREATE TABLE t_lwu_rename (x UInt32, v UInt32)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_lwu_rename', '1') ORDER BY tuple()
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, apply_patches_on_merge = 0, max_replicated_mutations_in_queue = 0, merge_selecting_sleep_ms = 100, max_merge_selecting_sleep_ms = 200;

INSERT INTO t_lwu_rename VALUES (1, 1), (2, 2);
UPDATE t_lwu_rename SET v = 5 WHERE x = 1;
ALTER TABLE t_lwu_rename RENAME COLUMN v TO w SETTINGS alter_sync = 0;
SYSTEM SYNC REPLICA t_lwu_rename;
UPDATE t_lwu_rename SET w = 7 WHERE x = 2;
OPTIMIZE TABLE t_lwu_rename FINAL;
SELECT 'after merge', x, w FROM t_lwu_rename ORDER BY x;
ALTER TABLE t_lwu_rename MODIFY SETTING max_replicated_mutations_in_queue = 16;
ALTER TABLE t_lwu_rename DELETE WHERE 0 SETTINGS mutations_sync = 2;
SELECT 'after mutation', x, w FROM t_lwu_rename ORDER BY x;
SELECT 'on disk', x, w FROM t_lwu_rename ORDER BY x SETTINGS apply_patch_parts = 0;
DROP TABLE t_lwu_rename SYNC;

-- The same after an earlier metadata-only ALTER.
DROP TABLE IF EXISTS t_lwu_rename_altered SYNC;
CREATE TABLE t_lwu_rename_altered (x UInt32, v UInt32)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_lwu_rename_altered', '1') ORDER BY tuple()
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, apply_patches_on_merge = 0, max_replicated_mutations_in_queue = 0, merge_selecting_sleep_ms = 100, max_merge_selecting_sleep_ms = 200;

INSERT INTO t_lwu_rename_altered VALUES (1, 1);
ALTER TABLE t_lwu_rename_altered ADD COLUMN z UInt8;
SELECT 'mutations of add column', count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_lwu_rename_altered';
UPDATE t_lwu_rename_altered SET v = 5 WHERE 1;
ALTER TABLE t_lwu_rename_altered RENAME COLUMN v TO w SETTINGS alter_sync = 0;
SYSTEM SYNC REPLICA t_lwu_rename_altered;
OPTIMIZE TABLE t_lwu_rename_altered FINAL;
SELECT 'altered after merge', x, w FROM t_lwu_rename_altered ORDER BY x;
ALTER TABLE t_lwu_rename_altered MODIFY SETTING max_replicated_mutations_in_queue = 16;
ALTER TABLE t_lwu_rename_altered DELETE WHERE 0 SETTINGS mutations_sync = 2;
SELECT 'altered on disk', x, w FROM t_lwu_rename_altered ORDER BY x SETTINGS apply_patch_parts = 0;
DROP TABLE t_lwu_rename_altered SYNC;

-- A merge that applies the patch to a part already at the new metadata version.
DROP TABLE IF EXISTS t_lwu_rename_merge SYNC;
CREATE TABLE t_lwu_rename_merge (x UInt32, v UInt32)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_lwu_rename_merge', '1') ORDER BY tuple()
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, apply_patches_on_merge = 0, max_replicated_mutations_in_queue = 0, merge_selecting_sleep_ms = 100, max_merge_selecting_sleep_ms = 200;

INSERT INTO t_lwu_rename_merge VALUES (1, 1);
UPDATE t_lwu_rename_merge SET v = 5 WHERE 1;
ALTER TABLE t_lwu_rename_merge RENAME COLUMN v TO w SETTINGS alter_sync = 0;
SYSTEM SYNC REPLICA t_lwu_rename_merge;
OPTIMIZE TABLE t_lwu_rename_merge FINAL;
ALTER TABLE t_lwu_rename_merge MODIFY SETTING apply_patches_on_merge = 1;
OPTIMIZE TABLE t_lwu_rename_merge FINAL;
SELECT 'merged with patch', x, w FROM t_lwu_rename_merge ORDER BY x SETTINGS apply_patch_parts = 0;
ALTER TABLE t_lwu_rename_merge MODIFY SETTING max_replicated_mutations_in_queue = 16;
ALTER TABLE t_lwu_rename_merge DELETE WHERE 0 SETTINGS mutations_sync = 2;
SELECT 'merged on disk', x, w FROM t_lwu_rename_merge ORDER BY x SETTINGS apply_patch_parts = 0;
DROP TABLE t_lwu_rename_merge SYNC;

-- The same for an added column whose only values are in the patch.
DROP TABLE IF EXISTS t_lwu_rename_added SYNC;
CREATE TABLE t_lwu_rename_added (x UInt32)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_lwu_rename_added', '1') ORDER BY tuple()
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, apply_patches_on_merge = 0, max_replicated_mutations_in_queue = 0, merge_selecting_sleep_ms = 100, max_merge_selecting_sleep_ms = 200;

INSERT INTO t_lwu_rename_added VALUES (1), (2);
ALTER TABLE t_lwu_rename_added ADD COLUMN v UInt32;
UPDATE t_lwu_rename_added SET v = 5 WHERE x = 1;
ALTER TABLE t_lwu_rename_added RENAME COLUMN v TO w SETTINGS alter_sync = 0;
SYSTEM SYNC REPLICA t_lwu_rename_added;
OPTIMIZE TABLE t_lwu_rename_added FINAL;
ALTER TABLE t_lwu_rename_added MODIFY SETTING apply_patches_on_merge = 1;
OPTIMIZE TABLE t_lwu_rename_added FINAL;
SELECT 'added merged with patch', x, w FROM t_lwu_rename_added ORDER BY x SETTINGS apply_patch_parts = 0;
ALTER TABLE t_lwu_rename_added MODIFY SETTING max_replicated_mutations_in_queue = 16;
ALTER TABLE t_lwu_rename_added DELETE WHERE 0 SETTINGS mutations_sync = 2;
SELECT 'added on disk', x, w FROM t_lwu_rename_added ORDER BY x SETTINGS apply_patch_parts = 0;
DROP TABLE t_lwu_rename_added SYNC;

-- An UPDATE by the old name on a replica that has not applied the rename yet.
DROP TABLE IF EXISTS t_lwu_rename_stale SYNC;
CREATE TABLE t_lwu_rename_stale (x UInt32, v UInt32)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_lwu_rename_stale', '1') ORDER BY tuple()
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, apply_patches_on_merge = 0, max_replicated_mutations_in_queue = 0, merge_selecting_sleep_ms = 100, max_merge_selecting_sleep_ms = 200;

INSERT INTO t_lwu_rename_stale VALUES (1, 1);
ALTER TABLE t_lwu_rename_stale ADD COLUMN z UInt8;
SYSTEM STOP REPLICATION QUEUES t_lwu_rename_stale;
ALTER TABLE t_lwu_rename_stale RENAME COLUMN v TO w SETTINGS alter_sync = 0;
UPDATE t_lwu_rename_stale SET v = 5 WHERE 1;
SYSTEM START REPLICATION QUEUES t_lwu_rename_stale;
SYSTEM SYNC REPLICA t_lwu_rename_stale;
OPTIMIZE TABLE t_lwu_rename_stale FINAL;
SELECT 'stale after merge', x, w FROM t_lwu_rename_stale ORDER BY x;
ALTER TABLE t_lwu_rename_stale MODIFY SETTING max_replicated_mutations_in_queue = 16;
ALTER TABLE t_lwu_rename_stale DELETE WHERE 0 SETTINGS mutations_sync = 2;
SELECT 'stale on disk', x, w FROM t_lwu_rename_stale ORDER BY x SETTINGS apply_patch_parts = 0;
DROP TABLE t_lwu_rename_stale SYNC;
