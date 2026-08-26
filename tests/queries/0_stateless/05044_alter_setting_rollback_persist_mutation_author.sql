-- Tags: zookeeper, no-parallel, no-replicated-database
-- Tag no-parallel: uses a failpoint, which affects the whole server.
-- Tag no-replicated-database: the durable metadata commit lives in ZooKeeper there and follows a different path.

-- `persist_mutation_author` gates the format of the serialized mutation entries, so a settings `ALTER`
-- that failed must not leave the table with the setting applied in memory only: later mutations would
-- then write entries in a format that the metadata on disk (and, for `ReplicatedMergeTree`, the other
-- replicas) do not expect. `alter` applies the settings before the durable metadata write, so it has to
-- restore them when that write throws.

DROP TABLE IF EXISTS t_alter_setting_rollback;
DROP TABLE IF EXISTS t_alter_setting_rollback_replicated;

CREATE TABLE t_alter_setting_rollback (id UInt64, value String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_alter_setting_rollback_replicated (id UInt64, value String)
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/t_alter_setting_rollback_replicated', '1') ORDER BY id;

INSERT INTO t_alter_setting_rollback VALUES (1, 'a');
INSERT INTO t_alter_setting_rollback_replicated VALUES (1, 'a');

SELECT '-- MergeTree';

SYSTEM ENABLE FAILPOINT alter_settings_throw_before_metadata_write;
ALTER TABLE t_alter_setting_rollback MODIFY SETTING persist_mutation_author = 1; -- { serverError FAULT_INJECTED }
SYSTEM DISABLE FAILPOINT alter_settings_throw_before_metadata_write;

-- The setting must not have been applied, so the mutation records no author.
ALTER TABLE t_alter_setting_rollback UPDATE value = 'b' WHERE id = 1 SETTINGS mutations_sync = 1;
SELECT author = '' FROM system.mutations WHERE database = currentDatabase() AND table = 't_alter_setting_rollback' ORDER BY mutation_id;

-- Without the injected failure the setting is applied and the author is recorded.
ALTER TABLE t_alter_setting_rollback MODIFY SETTING persist_mutation_author = 1;
ALTER TABLE t_alter_setting_rollback UPDATE value = 'c' WHERE id = 1 SETTINGS mutations_sync = 1;
SELECT author = currentUser() FROM system.mutations WHERE database = currentDatabase() AND table = 't_alter_setting_rollback' ORDER BY mutation_id DESC LIMIT 1;

SELECT '-- ReplicatedMergeTree';

SYSTEM ENABLE FAILPOINT alter_settings_throw_before_metadata_write;
ALTER TABLE t_alter_setting_rollback_replicated MODIFY SETTING persist_mutation_author = 1; -- { serverError FAULT_INJECTED }
SYSTEM DISABLE FAILPOINT alter_settings_throw_before_metadata_write;

ALTER TABLE t_alter_setting_rollback_replicated UPDATE value = 'b' WHERE id = 1 SETTINGS mutations_sync = 1;
SELECT author = '' FROM system.mutations WHERE database = currentDatabase() AND table = 't_alter_setting_rollback_replicated' ORDER BY mutation_id;

ALTER TABLE t_alter_setting_rollback_replicated MODIFY SETTING persist_mutation_author = 1;
ALTER TABLE t_alter_setting_rollback_replicated UPDATE value = 'c' WHERE id = 1 SETTINGS mutations_sync = 1;
SELECT author = currentUser() FROM system.mutations WHERE database = currentDatabase() AND table = 't_alter_setting_rollback_replicated' ORDER BY mutation_id DESC LIMIT 1;

DROP TABLE t_alter_setting_rollback;
DROP TABLE t_alter_setting_rollback_replicated;
