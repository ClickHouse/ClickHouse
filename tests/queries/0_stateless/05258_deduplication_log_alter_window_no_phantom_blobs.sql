-- Tags: no-fasttest, no-shared-catalog, no-async-insert
-- no-fasttest: requires the S3 disk (minio)
-- no-shared-catalog: uses a plain (non-replicated) MergeTree table on an S3 disk
-- no-async-insert: async inserts compute deduplication block ids differently

-- Changing `non_replicated_deduplication_window` with `ALTER TABLE ... MODIFY SETTING` must not open an
-- appending writer for the unfinished deduplication log: finalizing that writer on `DETACH` without any
-- insert in between registers a phantom blob in the log file's metadata without uploading any object,
-- and the next load of the log fails to read it (`NoSuchKey` on S3), logging an
-- "Error while loading MergeTree deduplication log" error. This is the `setDeduplicationWindowSize`
-- counterpart of `04640_deduplication_log_reattach_no_phantom_blobs`, which covers the `load` path.

DROP TABLE IF EXISTS t_dedup_log_alter_window;

CREATE TABLE t_dedup_log_alter_window (x UInt64) ENGINE = MergeTree ORDER BY x
SETTINGS disk = 's3_disk', non_replicated_deduplication_window = 100;

INSERT INTO t_dedup_log_alter_window VALUES (1);

-- After the reattach the current log is unfinished and has no open writer.
DETACH TABLE t_dedup_log_alter_window;
ATTACH TABLE t_dedup_log_alter_window;

ALTER TABLE t_dedup_log_alter_window MODIFY SETTING non_replicated_deduplication_window = 200;
DETACH TABLE t_dedup_log_alter_window;
ATTACH TABLE t_dedup_log_alter_window;

ALTER TABLE t_dedup_log_alter_window MODIFY SETTING non_replicated_deduplication_window = 100;
DETACH TABLE t_dedup_log_alter_window;
ATTACH TABLE t_dedup_log_alter_window;

-- The insert must still be deduplicated.
INSERT INTO t_dedup_log_alter_window VALUES (1);
SELECT count() FROM t_dedup_log_alter_window;

-- No ATTACH may have failed to load the deduplication log. The log path in the message contains the
-- table UUID, so the check is scoped to this table only.
SYSTEM FLUSH LOGS text_log;
SELECT count() FROM system.text_log
WHERE level = 'Error'
    AND message LIKE '%Error while loading MergeTree deduplication log%'
    AND message LIKE '%' || (SELECT toString(uuid) FROM system.tables WHERE database = currentDatabase() AND name = 't_dedup_log_alter_window') || '%';

DROP TABLE t_dedup_log_alter_window;
