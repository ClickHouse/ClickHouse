-- Tags: no-fasttest, no-shared-catalog, no-async-insert
-- no-fasttest: requires the S3 disk (minio)
-- no-shared-catalog: uses a plain (non-replicated) MergeTree table on an S3 disk
-- no-async-insert: async inserts compute deduplication block ids differently

-- Finalizing an appending writer that has written nothing must not leave a phantom blob in the
-- deduplication log file's metadata: such a blob is registered without uploading any object, and
-- the next load of the log fails to read it (`NoSuchKey` on S3). Before the fix, every
-- DETACH/ATTACH cycle without inserts in between appended one phantom blob to the current log.

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

-- The current deduplication log must consist of exactly one blob (the one holding the record of
-- the insert above), not one real blob plus one phantom blob per reattach cycle.
SELECT count() FROM system.remote_data_paths
WHERE disk_name = 's3_disk'
    AND local_path LIKE '%' || (SELECT toString(uuid) FROM system.tables WHERE database = currentDatabase() AND name = 't_dedup_log_reattach') || '%deduplication_log_1%';

-- The insert must still be deduplicated after the reattach cycles (the log is loaded correctly).
INSERT INTO t_dedup_log_reattach VALUES (1);
SELECT count() FROM t_dedup_log_reattach;

DROP TABLE t_dedup_log_reattach;
