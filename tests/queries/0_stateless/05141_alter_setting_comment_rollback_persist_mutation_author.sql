-- Tags: zookeeper, no-parallel, no-replicated-database
-- Tag no-parallel: uses a failpoint, which affects the whole server.
-- Tag no-replicated-database: the durable metadata commit lives in ZooKeeper there and follows a different path.

-- An `ALTER` that mixes `MODIFY SETTING` with `MODIFY COMMENT` is still applied locally, but it takes
-- its own branch in `StorageReplicatedMergeTree::alter`. That branch must roll the settings back when
-- the durable metadata write throws, exactly like a pure settings `ALTER`: otherwise a failed
-- `MODIFY COMMENT ..., MODIFY SETTING persist_mutation_author = 1` would leave this replica writing
-- `/mutations` entries in a format the other replicas cannot read.

DROP TABLE IF EXISTS t_alter_setting_comment_rollback;

CREATE TABLE t_alter_setting_comment_rollback (id UInt64, value String)
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/t_alter_setting_comment_rollback', '1') ORDER BY id;

INSERT INTO t_alter_setting_comment_rollback VALUES (1, 'a');

SYSTEM ENABLE FAILPOINT alter_settings_throw_before_metadata_write;
ALTER TABLE t_alter_setting_comment_rollback MODIFY COMMENT 'failed', MODIFY SETTING persist_mutation_author = 1; -- { serverError FAULT_INJECTED }
SYSTEM DISABLE FAILPOINT alter_settings_throw_before_metadata_write;

-- Neither the setting nor the comment must have been applied.
SELECT comment FROM system.tables WHERE database = currentDatabase() AND table = 't_alter_setting_comment_rollback';

ALTER TABLE t_alter_setting_comment_rollback UPDATE value = 'b' WHERE id = 1 SETTINGS mutations_sync = 1;
SELECT author = '' FROM system.mutations WHERE database = currentDatabase() AND table = 't_alter_setting_comment_rollback' ORDER BY mutation_id;

-- Without the injected failure both changes are applied and the author is recorded.
ALTER TABLE t_alter_setting_comment_rollback MODIFY COMMENT 'applied', MODIFY SETTING persist_mutation_author = 1;
SELECT comment FROM system.tables WHERE database = currentDatabase() AND table = 't_alter_setting_comment_rollback';

ALTER TABLE t_alter_setting_comment_rollback UPDATE value = 'c' WHERE id = 1 SETTINGS mutations_sync = 1;
SELECT author = currentUser() FROM system.mutations WHERE database = currentDatabase() AND table = 't_alter_setting_comment_rollback' ORDER BY mutation_id DESC LIMIT 1;

DROP TABLE t_alter_setting_comment_rollback;
