-- Tags: no-fasttest
-- no-fasttest: Parquet and MinIO

-- A cached row count is returned as one chunk only when nothing reads the columns of the read.
-- Each memory limit fails if the read of that query gets all 10M rows (80 MB of UInt64) in one chunk.

-- `url()` and `s3()` are read locally: with parallel replicas their cluster form counts on the replicas, out of sight of the `query_log` check.
SET optimize_count_from_files = 1, use_cache_for_count_from_files = 1, optimize_trivial_count_query = 1, parallel_replicas_for_cluster_engines = 0;

INSERT INTO FUNCTION file(currentDatabase() || '_x.parquet', Parquet, 'x UInt64') SELECT 0 FROM numbers(10000000) SETTINGS engine_file_truncate_on_insert = 1;
INSERT INTO FUNCTION s3(s3_conn, filename = currentDatabase() || '_x.parquet', format = Parquet, structure = 'x UInt64') SELECT 0 FROM numbers(10000000) SETTINGS s3_truncate_on_insert = 1;
INSERT INTO FUNCTION file(currentDatabase() || '_hive/a=1/data.parquet', Parquet, 's String') SELECT '' FROM numbers(10000000) SETTINGS engine_file_truncate_on_insert = 1;
INSERT INTO FUNCTION file(currentDatabase() || '_empty.parquet', Parquet, 'x UInt64') SELECT number FROM numbers(0) SETTINGS engine_file_truncate_on_insert = 1;
CREATE TABLE file_x (x UInt64) ENGINE = File(Parquet);
INSERT INTO file_x SELECT 0 FROM numbers(10000000);

-- The row count cache ignores entries registered in the same second as the last modification of the file.
SELECT sleep(1) FORMAT Null;

SELECT count() FROM file(currentDatabase() || '_x.parquet') FORMAT Null;
SELECT count() FROM file(currentDatabase() || '_x.parquet') SETTINGS log_comment = 'one_chunk_file';

SELECT count() FROM url('http://localhost:11111/test/' || currentDatabase() || '_x.parquet') FORMAT Null;
SELECT count() FROM url('http://localhost:11111/test/' || currentDatabase() || '_x.parquet') SETTINGS log_comment = 'one_chunk_url';

-- Object storage converts the constant columns of every other chunk to full columns.
SELECT count() FROM s3(s3_conn, filename = currentDatabase() || '_x.parquet') FORMAT Null;
SELECT count() FROM s3(s3_conn, filename = currentDatabase() || '_x.parquet') SETTINGS max_memory_usage = '40Mi', log_comment = 'one_chunk_s3';

-- The aggregation materializes the argument of count(x).
SELECT count(x) FROM file(currentDatabase() || '_x.parquet') FORMAT Null;
SELECT count(x) FROM file(currentDatabase() || '_x.parquet') SETTINGS max_memory_usage = '40Mi', log_comment = 'one_chunk_count_x';

-- Merge materializes the columns of the reads of its tables.
SELECT count() FROM merge(currentDatabase(), '^file_x$') FORMAT Null;
SELECT count() FROM merge(currentDatabase(), '^file_x$') SETTINGS max_memory_usage = '40Mi', log_comment = 'one_chunk_merge';

-- The read returns only the hive partition column `a`, which is added to every chunk as a full column.
SELECT count() FROM file(currentDatabase() || '_hive/a=1/data.parquet') SETTINGS use_hive_partitioning = 1 FORMAT Null;
SELECT count() FROM file(currentDatabase() || '_hive/a=1/data.parquet') SETTINGS use_hive_partitioning = 1, max_memory_usage = '40Mi', log_comment = 'one_chunk_hive';

SELECT count() FROM file(currentDatabase() || '_empty.parquet');
SELECT count() FROM file(currentDatabase() || '_empty.parquet') SETTINGS log_comment = 'one_chunk_empty';

-- Every query above took its row count from the cache.
SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['SchemaInferenceCacheNumRowsHits'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND startsWith(log_comment, 'one_chunk_')
ORDER BY log_comment;
