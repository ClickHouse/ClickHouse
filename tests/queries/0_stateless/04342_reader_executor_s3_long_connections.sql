-- Tags: no-fasttest, no-distributed-cache, no-encrypted-storage, no-parallel-replicas
-- The experimental ReaderExecutor's long-connection reuse on a DIRECT object-storage read (the
-- s3() table function / StorageObjectStorage), not via a MergeTree disk. The held source connection
-- is opened here only because the limit is wired into the direct read path
-- (StorageObjectStorageSource::createReadBuffer), not just DiskObjectStorage::prepareRead.
--   no-fasttest: needs minio (object storage).
--   no-distributed-cache / no-encrypted-storage: the executor falls back on those stages, so its
--   metrics would not be emitted there.
--   no-parallel-replicas: distributed reading doesn't open a held connection on the observed path.

INSERT INTO FUNCTION s3(s3_conn, filename = '04342_reader_executor_long_conn', format = Parquet)
SELECT number AS id, number * 2 AS v, repeat('x', 64) AS s FROM numbers(500000)
SETTINGS s3_truncate_on_insert = 1;

SET use_reader_executor = 1;
SET remote_filesystem_read_method = 'read';   -- avoid the async-prefetch stage
SET enable_filesystem_cache = 0;               -- avoid the filesystem-cache stage so the executor engages
SET max_read_buffer_size = 65536;              -- small windows -> many sequential reads per object
SET max_threads = 1;

-- A direct s3() read opens a held source connection (the limit reaches the direct path now).
SELECT sum(id) + sum(v) + sum(length(s))
FROM s3(s3_conn, filename = '04342_reader_executor_long_conn', format = Parquet,
        structure = 'id UInt64, v UInt64, s String')
SETTINGS reader_executor_use_long_connections = 1, log_comment = '04342_s3_long_conn' FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['ReaderExecutorLongConnectionOpened'] > 0
FROM system.query_log
WHERE log_comment = '04342_s3_long_conn' AND type = 'QueryFinish' AND current_database = currentDatabase()
ORDER BY event_time_microseconds DESC LIMIT 1;
