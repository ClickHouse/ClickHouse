-- Tags: no-fasttest, no-shared-catalog, no-async-insert
-- no-fasttest: requires the S3 disk (minio)
-- no-shared-catalog: uses a plain (non-replicated) MergeTree table on an S3 disk
-- no-async-insert: async inserts compute deduplication block ids differently

-- Finalizing an appending writer that has written nothing must not leave a phantom blob in the
-- deduplication log file's metadata: such a blob is registered without uploading any object, and
-- the next load of the log fails to read it (`NoSuchKey` on S3), logging an
-- "Error while loading MergeTree deduplication log" error. Before the fix, every DETACH/ATTACH
-- cycle without inserts in between appended one phantom blob to the current log, so the second
-- and third ATTACH below logged that error.

DROP TABLE IF EXISTS t_dedup_log_reattach;

CREATE TABLE t_dedup_log_reattach (x UInt64) ENGINE = MergeTree ORDER BY x
SETTINGS disk = 's3_disk', non_replicated_deduplication_window = 100;

INSERT INTO t_dedup_log_reattach VALUES (1);

DETACH TABLE t_dedup_log_reattach;
ATTACH TABLE t_dedup_log_reattach;
DETACH TABLE t_dedup_log_reattach;
ATTACH TABLE t_dedup_log_reattach;
DETACH TABLE t_dedup_log_reattach;
ATTACH TABLE t_dedup_log_reattach;

-- The insert must still be deduplicated after the reattach cycles (the log is loaded correctly).
INSERT INTO t_dedup_log_reattach VALUES (1);
SELECT count() FROM t_dedup_log_reattach;

-- No ATTACH may have failed to load the deduplication log. The log path in the message contains the
-- table UUID, so the check is scoped to this table only. (This intentionally avoids counting the log's
-- blobs through `system.remote_data_paths`, which enumerates every remote path on every disk and is far
-- too slow on heavily loaded CI servers.)
SYSTEM FLUSH LOGS text_log;
SELECT count() FROM system.text_log
WHERE level = 'Error'
    AND message LIKE '%Error while loading MergeTree deduplication log%'
    AND message LIKE '%' || (SELECT toString(uuid) FROM system.tables WHERE database = currentDatabase() AND name = 't_dedup_log_reattach') || '%';

DROP TABLE t_dedup_log_reattach;
