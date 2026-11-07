-- Tags: zookeeper, no-replicated-database, no-shared-merge-tree
-- no-replicated-database: relies on alter_sync = 0 and SYSTEM SYNC REPLICA ordering of a single replica
-- no-shared-merge-tree: relies on max_replicated_mutations_in_queue
-- A mutation queued before RENAME COLUMN must keep the renamed column's data.

SET enable_lightweight_update = 1;

-- ALTER DELETE, compact part.
DROP TABLE IF EXISTS t_compact SYNC;
CREATE TABLE t_compact (id UInt32, a UInt32, b UInt32)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_compact', '1') ORDER BY tuple()
SETTINGS max_replicated_mutations_in_queue = 0, min_bytes_for_wide_part = '10G', min_rows_for_wide_part = 1000000000;

INSERT INTO t_compact VALUES (1, 1111, 2222), (2, 3333, 4444);
ALTER TABLE t_compact DELETE WHERE id = 1 SETTINGS mutations_sync = 0;
ALTER TABLE t_compact RENAME COLUMN b TO c SETTINGS alter_sync = 0;
SYSTEM SYNC REPLICA t_compact;
SELECT 'compact pending', count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_compact' AND NOT is_done;
SELECT 'compact columns', groupArray(name) FROM (SELECT name FROM system.columns WHERE database = currentDatabase() AND table = 't_compact' ORDER BY position);
ALTER TABLE t_compact MODIFY SETTING max_replicated_mutations_in_queue = 16;
ALTER TABLE t_compact UPDATE a = a WHERE 1 SETTINGS mutations_sync = 2;
SELECT 'compact', id, a, c FROM t_compact ORDER BY id;
DROP TABLE t_compact SYNC;

-- ALTER DELETE, wide part.
DROP TABLE IF EXISTS t_wide SYNC;
CREATE TABLE t_wide (id UInt32, a UInt32, b UInt32)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_wide', '1') ORDER BY tuple()
SETTINGS max_replicated_mutations_in_queue = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

INSERT INTO t_wide VALUES (1, 1111, 2222), (2, 3333, 4444);
ALTER TABLE t_wide DELETE WHERE id = 1 SETTINGS mutations_sync = 0;
ALTER TABLE t_wide RENAME COLUMN b TO c SETTINGS alter_sync = 0;
SYSTEM SYNC REPLICA t_wide;
SELECT 'wide pending', count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_wide' AND NOT is_done;
SELECT 'wide columns', groupArray(name) FROM (SELECT name FROM system.columns WHERE database = currentDatabase() AND table = 't_wide' ORDER BY position);
ALTER TABLE t_wide MODIFY SETTING max_replicated_mutations_in_queue = 16;
ALTER TABLE t_wide UPDATE a = a WHERE 1 SETTINGS mutations_sync = 2;
SELECT 'wide', id, a, c FROM t_wide ORDER BY id;
DROP TABLE t_wide SYNC;

-- ALTER UPDATE of another column, wide part.
DROP TABLE IF EXISTS t_update SYNC;
CREATE TABLE t_update (id UInt32, a UInt32, b UInt32)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_update', '1') ORDER BY tuple()
SETTINGS max_replicated_mutations_in_queue = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

INSERT INTO t_update VALUES (1, 1111, 2222), (2, 3333, 4444);
ALTER TABLE t_update UPDATE a = a + 1 WHERE 1 SETTINGS mutations_sync = 0;
ALTER TABLE t_update RENAME COLUMN b TO c SETTINGS alter_sync = 0;
SYSTEM SYNC REPLICA t_update;
SELECT 'update pending', count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_update' AND NOT is_done;
SELECT 'update columns', groupArray(name) FROM (SELECT name FROM system.columns WHERE database = currentDatabase() AND table = 't_update' ORDER BY position);
ALTER TABLE t_update MODIFY SETTING max_replicated_mutations_in_queue = 16;
ALTER TABLE t_update UPDATE a = a WHERE 1 SETTINGS mutations_sync = 2;
SELECT 'update', id, a, c FROM t_update ORDER BY id;
CHECK TABLE t_update SETTINGS check_query_single_value_result = 1;
DROP TABLE t_update SYNC;

-- Lightweight UPDATE of the renamed column, then ALTER DELETE.
DROP TABLE IF EXISTS t_patch SYNC;
CREATE TABLE t_patch (id UInt32, a UInt32, b UInt32)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_patch', '1') ORDER BY tuple()
SETTINGS max_replicated_mutations_in_queue = 0, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO t_patch VALUES (1, 1111, 2222), (2, 3333, 4444);
UPDATE t_patch SET b = 9999 WHERE id = 2;
ALTER TABLE t_patch DELETE WHERE id = 1 SETTINGS mutations_sync = 0;
ALTER TABLE t_patch RENAME COLUMN b TO c SETTINGS alter_sync = 0;
SYSTEM SYNC REPLICA t_patch;
SELECT 'patch pending', count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_patch' AND NOT is_done;
SELECT 'patch columns', groupArray(name) FROM (SELECT name FROM system.columns WHERE database = currentDatabase() AND table = 't_patch' ORDER BY position);
ALTER TABLE t_patch MODIFY SETTING max_replicated_mutations_in_queue = 16;
ALTER TABLE t_patch UPDATE a = a WHERE 1 SETTINGS mutations_sync = 2;
SELECT 'patch', id, a, c FROM t_patch ORDER BY id;
SELECT 'patch on disk', id, a, c FROM t_patch ORDER BY id SETTINGS apply_patch_parts = 0;
DROP TABLE t_patch SYNC;

-- MATERIALIZE COLUMN queued before CLEAR COLUMN of the column its default reads.
DROP TABLE IF EXISTS t_clear SYNC;
CREATE TABLE t_clear (id UInt32, a UInt32, b UInt32)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_clear', '1') ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0, replicated_max_mutations_in_one_entry = 1;

INSERT INTO t_clear VALUES (1, 1111, 2222), (2, 3333, 4444);
ALTER TABLE t_clear ADD COLUMN d UInt32 DEFAULT b * 2 SETTINGS alter_sync = 2;
ALTER TABLE t_clear MODIFY SETTING max_replicated_mutations_in_queue = 0;
ALTER TABLE t_clear MATERIALIZE COLUMN d SETTINGS mutations_sync = 0;
ALTER TABLE t_clear CLEAR COLUMN b SETTINGS alter_sync = 0;
SYSTEM SYNC REPLICA t_clear;
SELECT 'clear pending', count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_clear' AND NOT is_done;
ALTER TABLE t_clear MODIFY SETTING max_replicated_mutations_in_queue = 16;
ALTER TABLE t_clear UPDATE a = a WHERE 1 SETTINGS mutations_sync = 2;
SELECT 'clear', id, b, d FROM t_clear ORDER BY id;
DROP TABLE t_clear SYNC;

-- ALTER UPDATE of another column on a part attached with the pre-rename column names.
DROP TABLE IF EXISTS t_attach SYNC;
CREATE TABLE t_attach (id UInt32, a UInt32, b UInt32)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_attach', '1') ORDER BY tuple() PARTITION BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

INSERT INTO t_attach VALUES (1, 1111, 2222), (2, 3333, 4444);
ALTER TABLE t_attach DETACH PARTITION tuple();
ALTER TABLE t_attach RENAME COLUMN b TO c SETTINGS alter_sync = 2;
ALTER TABLE t_attach ATTACH PARTITION tuple();
SELECT 'attach before', id, a, c FROM t_attach ORDER BY id;
ALTER TABLE t_attach UPDATE a = a + 1 WHERE 1 SETTINGS mutations_sync = 2;
SELECT 'attach', id, a, c FROM t_attach ORDER BY id;
CHECK TABLE t_attach SETTINGS check_query_single_value_result = 1;
DROP TABLE t_attach SYNC;

-- ALTER UPDATE of the renamed column itself on a part attached with the pre-rename column names.
DROP TABLE IF EXISTS t_attach_target SYNC;
CREATE TABLE t_attach_target (id UInt32, a UInt32, b UInt32)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_attach_target', '1') ORDER BY tuple() PARTITION BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

INSERT INTO t_attach_target VALUES (1, 1111, 2222), (2, 3333, 4444);
ALTER TABLE t_attach_target DETACH PARTITION tuple();
ALTER TABLE t_attach_target RENAME COLUMN b TO c SETTINGS alter_sync = 2;
ALTER TABLE t_attach_target ATTACH PARTITION tuple();
ALTER TABLE t_attach_target UPDATE c = c + 1 WHERE 1 SETTINGS mutations_sync = 2;
SELECT 'attach target', id, a, c FROM t_attach_target ORDER BY id;
CHECK TABLE t_attach_target SETTINGS check_query_single_value_result = 1;
DROP TABLE t_attach_target SYNC;
