-- Tags: no-fasttest
-- ^ cas is an object-storage metadata type; keep it off the minimal fasttest image.

-- A non_replicated_deduplication_window > 0 on a plain MergeTree keeps an on-disk deduplication log
-- (deduplication_logs/deduplication_log_N.txt) at the table root. On a cas disk that log
-- works the same way it does on a plain s3 disk: the disk cannot host append writes, so the log
-- rewrites a fresh rotated log object per record, stored verbatim in the table's files/ namespace. This
-- test uses an INLINE cas disk, so it exercises the CA path on any test config.

DROP TABLE IF EXISTS t_cas_deduplication;

CREATE TABLE t_cas_deduplication (a UInt64)
ENGINE = MergeTree ORDER BY a
SETTINGS non_replicated_deduplication_window = 100, disk = disk(
    type = object_storage,
    object_storage_type = local,
    metadata_type = cas,
    cas_server_root_id = '04285',
    name = '04285_cas_deduplication',
    path = '04285_cas_deduplication_pool/');

-- Identical inserts are deduplicated; each insert also writes a record to the on-disk log (the write
-- that used to fail closed with a null writer on a cas disk).
INSERT INTO t_cas_deduplication VALUES (1);
INSERT INTO t_cas_deduplication VALUES (1);
SELECT 'after-two-identical', count() FROM t_cas_deduplication;

-- A distinct block is accepted.
INSERT INTO t_cas_deduplication VALUES (2);
SELECT 'after-new-block', count() FROM t_cas_deduplication;

-- Reload the table: the deduplication log is re-read from the cas disk.
DETACH TABLE t_cas_deduplication;
ATTACH TABLE t_cas_deduplication;

-- The first block is still deduplicated — its record was reloaded from the on-disk log, not memory.
INSERT INTO t_cas_deduplication VALUES (1);
SELECT 'after-reload-same-block', count() FROM t_cas_deduplication;

-- A new distinct block is still accepted after the reload.
INSERT INTO t_cas_deduplication VALUES (3);
SELECT 'after-reload-new-block', count() FROM t_cas_deduplication;

DROP TABLE t_cas_deduplication;
SELECT 'dropped_ok';
