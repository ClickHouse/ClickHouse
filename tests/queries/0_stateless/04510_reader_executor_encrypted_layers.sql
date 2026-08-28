-- Tags: no-fasttest, no-distributed-cache, no-encrypted-storage, no-parallel-replicas
-- Two stacked encryption layers (DiskEncrypted over DiskEncrypted over raw object storage): the
-- ReaderExecutor must decrypt both layers to return correct data, and its multi-layer path engages
-- end-to-end here (each DiskEncrypted layer adds a decryption stage to the pipeline).
--   no-fasttest: needs minio (object storage).
--   no-distributed-cache: the executor falls back on the distributed-cache stage.
--   no-encrypted-storage: this test pins its own encrypted policy; the encrypted-storage CI variant
--     would add yet another encryption layer, which is not what this test targets.
--   no-parallel-replicas: distributed reading does not take the observed local read path.

DROP TABLE IF EXISTS t_reader_executor_encrypted_layers;

CREATE TABLE t_reader_executor_encrypted_layers
(
    id UInt64,
    v UInt64,
    s String
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192, storage_policy = 's3_no_cache_encrypted_double';

INSERT INTO t_reader_executor_encrypted_layers
SELECT number, number * 2, concat('row_', toString(number))
FROM numbers(300000);

SET use_reader_executor = 1;
SET remote_filesystem_read_method = 'read';
SET enable_filesystem_cache = 0;

SELECT count(), sum(id), sum(v) FROM t_reader_executor_encrypted_layers SETTINGS log_comment = '04510_reader_executor_encrypted_layers_probe';
SELECT id, v, s FROM t_reader_executor_encrypted_layers WHERE id = 150000;
SELECT count(), min(id), max(id) FROM t_reader_executor_encrypted_layers WHERE id BETWEEN 100000 AND 100099;
SELECT s FROM t_reader_executor_encrypted_layers WHERE id = 299999;
SELECT sum(length(s)) FROM t_reader_executor_encrypted_layers;

-- Activation check: the executor path was taken over the doubly-encrypted disk.
SYSTEM FLUSH LOGS query_log, text_log;

SELECT count() > 0
FROM system.text_log
WHERE logger_name = 'ReadPipeline'
  AND message LIKE '%using ReaderExecutor%'
  AND query_id IN (
      SELECT query_id
      FROM system.query_log
      WHERE log_comment = '04510_reader_executor_encrypted_layers_probe'
        AND type = 'QueryFinish'
        AND current_database = currentDatabase()
  );

DROP TABLE t_reader_executor_encrypted_layers;
