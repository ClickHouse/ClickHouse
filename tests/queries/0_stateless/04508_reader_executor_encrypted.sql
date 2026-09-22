-- Tags: no-fasttest, no-distributed-cache, no-encrypted-storage, no-parallel-replicas
-- The experimental ReaderExecutor now decrypts served payload itself, so it engages on an
-- ENCRYPTED object-storage disk (no filesystem cache) instead of falling back. This checks the
-- data comes back correct AND that the executor path was taken over encrypted storage.
--   no-fasttest: needs minio (object storage).
--   no-distributed-cache: the executor falls back on the distributed-cache stage.
--   no-encrypted-storage: this test pins its own encrypted policy; the encrypted-storage CI variant
--     would wrap the disk in a second encryption layer, which is not what this test targets.
--   no-parallel-replicas: distributed reading does not take the observed local read path.

DROP TABLE IF EXISTS t_reader_executor_encrypted;

CREATE TABLE t_reader_executor_encrypted
(
    id UInt64,
    v UInt64,
    s String
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192, storage_policy = 's3_no_cache_encrypted';

-- Enough rows that column .bin files span many read blocks.
INSERT INTO t_reader_executor_encrypted
SELECT number, number * 2, concat('row_', toString(number))
FROM numbers(300000);

SET use_reader_executor = 1;
-- The executor falls back when a stage it does not implement is configured. Disable the
-- async-prefetch and filesystem-cache stages so it actually runs; decryption is now handled
-- internally, so the encrypted disk no longer forces a fallback.
SET remote_filesystem_read_method = 'read';
SET enable_filesystem_cache = 0;

-- Full scan over numeric columns. The `log_comment` marks this query for the activation check.
SELECT count(), sum(id), sum(v) FROM t_reader_executor_encrypted SETTINGS log_comment = '04508_reader_executor_encrypted_probe';

-- Point lookup: seek to a single granule and read one row.
SELECT id, v, s FROM t_reader_executor_encrypted WHERE id = 150000;

-- Bounded range read.
SELECT count(), min(id), max(id) FROM t_reader_executor_encrypted WHERE id BETWEEN 100000 AND 100099;

-- String column read at the tail of the data.
SELECT s FROM t_reader_executor_encrypted WHERE id = 299999;

-- Force a full scan of the string column.
SELECT sum(length(s)) FROM t_reader_executor_encrypted;

-- Activation check: `ReadPipeline::build` logs `using ReaderExecutor ...` at DEBUG when the
-- executor path is chosen. Confirm at least one such line was emitted for the marked query over the
-- encrypted disk (correlated by query id, scoped to this test's database). Prints 1 when active.
SYSTEM FLUSH LOGS query_log, text_log;

SELECT count() > 0
FROM system.text_log
WHERE logger_name = 'ReadPipeline'
  AND message LIKE '%using ReaderExecutor%'
  AND query_id IN (
      SELECT query_id
      FROM system.query_log
      WHERE log_comment = '04508_reader_executor_encrypted_probe'
        AND type = 'QueryFinish'
        AND current_database = currentDatabase()
  );

DROP TABLE t_reader_executor_encrypted;
