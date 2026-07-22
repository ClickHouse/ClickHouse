-- Regression: a `Remote` storage engine with explicit columns over a local-shard table-function
-- target must be restorable from a backup even when the target's source table is absent in the
-- restore environment. The restore must trust the persisted columns instead of re-analyzing the
-- table-function target (which would throw because the source is gone).
--
-- Before the fix, a normal `RESTORE` reached `StorageFactory` with `SECONDARY_CREATE` and
-- `is_restore_from_backup`, which `isLoadingFromExistingMetadata` did not cover, so the
-- table-function target was re-analyzed on restore and the restore of a valid table failed.

DROP TABLE IF EXISTS remote_restore_src SYNC;
DROP TABLE IF EXISTS remote_restore_t SYNC;

CREATE TABLE remote_restore_src (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO remote_restore_src VALUES (42);

-- A local table-function target whose analysis depends on `remote_restore_src`.
CREATE TABLE remote_restore_t (x UInt64)
    ENGINE = Remote('127.0.0.1', merge(currentDatabase(), '^remote_restore_src$'));

SELECT x FROM remote_restore_t ORDER BY x;

BACKUP TABLE remote_restore_t TO Memory('04341_remote_storage_engine_restore') FORMAT Null;

-- Drop the `Remote` table and the source its target reads from, so the source is absent at restore.
DROP TABLE remote_restore_t SYNC;
DROP TABLE remote_restore_src SYNC;

-- The restore must succeed even though `remote_restore_src` no longer exists: the table carries its
-- own explicit columns and must not re-analyze the table-function target just to be restored.
--
-- The tolerated fallback logs the swallowed analysis error at `WARNING` (the target's source is
-- intentionally absent here); silence it so the expected server-log message relayed to the client at
-- the default `send_logs_level=warning` does not trip the harness "having stderror" check.
SET send_logs_level = 'fatal';
RESTORE TABLE remote_restore_t FROM Memory('04341_remote_storage_engine_restore') FORMAT Null;

-- The restored table is present; it persists as a `Remote` engine definition (which builds a
-- `Distributed` storage at runtime). `engine_full` embeds the database name, so only check its prefix.
SELECT name, engine, startsWith(engine_full, 'Remote(') FROM system.tables WHERE database = currentDatabase() AND name = 'remote_restore_t';

DROP TABLE remote_restore_t SYNC;
