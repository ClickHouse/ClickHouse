-- Tags: no-fasttest, no-parallel-replicas, no-replicated-database
-- Tag no-fasttest: depends on MinIO via s3_conn for write and HTTP read, on Azurite, and on Iceberg (Avro).
-- Tag no-parallel-replicas: the assertions read ProfileEvents of the local query_log rows.
-- Tag no-replicated-database: the replicated DDL worker evaluates the `IcebergLocal` arguments without the session, so it does not see the `TEMPORARY TABLE iceberg_path`
-- Random settings limits: max_block_size=(10000, None)
-- max_block_size: `_1.parquet` has 10 row groups of 1000 rows (10 `s3Cluster` buckets) only when its 10000 rows are written as one block.

-- `min` / `max` of Parquet columns read from object storage (`s3`, `azureBlobStorage`) or over HTTP (`url`) come from
-- the column chunk statistics (`ParquetReadMinMaxFromStatistics` = 1 per file) with the same values as reading the data;
-- every other case reads the data (0). The `file` variant is 05331_parquet_min_max_from_statistics.

SET s3_truncate_on_insert = 1;
SET allow_insert_into_iceberg = 1;
SET use_iceberg_metadata_files_cache = 0;
SET iceberg_delete_data_on_drop = 0;
SET async_insert = 0;

INSERT INTO FUNCTION s3(s3_conn, filename = '05366/' || currentDatabase() || '_1.parquet', format = Parquet)
SELECT
    number AS i,
    toInt64(number) - 5000 AS s,
    toDate('2020-01-01') + number % 3000 AS d,
    toDecimal64(number, 3) / 7 AS dec,
    if(number % 3 = 0, NULL, toInt64(number) * 2 - 7000) AS n,
    toString(number) AS str
FROM numbers(10000)
SETTINGS output_format_parquet_row_group_size = 1000;

INSERT INTO FUNCTION s3(s3_conn, filename = '05366/' || currentDatabase() || '_2.parquet', format = Parquet)
SELECT number + 20000 AS i FROM numbers(100);

INSERT INTO FUNCTION s3(s3_conn, filename = '05366/' || currentDatabase() || '_hive/p=7/data.parquet', format = Parquet)
SELECT number AS i FROM numbers(100);

INSERT INTO FUNCTION azureBlobStorage('DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://localhost:10000/devstoreaccount1;', 'cont05366', currentDatabase() || '_1.parquet', 'Parquet')
SELECT number AS i, toDate('2020-01-01') + number % 3000 AS d FROM numbers(10000)
SETTINGS azure_truncate_on_insert = 1, output_format_parquet_row_group_size = 1000;

CREATE TABLE t_s3_policy (i UInt64) ENGINE = S3(s3_conn, filename = '05366/' || currentDatabase() || '_1.parquet', format = Parquet);
CREATE TABLE t_url_policy (i UInt64) ENGINE = URL('http://localhost:11111/test/05366/' || currentDatabase() || '_1.parquet', Parquet);
CREATE ROW POLICY p_05366_s3 ON t_s3_policy USING i >= 100 AND i < 900 TO ALL;
CREATE ROW POLICY p_05366_url ON t_url_policy USING i >= 100 AND i < 900 TO ALL;

-- Resolve relative user-files paths against the server's data directory.
CREATE TEMPORARY TABLE iceberg_path AS
WITH if(changed, trimBoth(value), 'user_files/') AS user_files_path
SELECT concat(
    if(startsWith(user_files_path, '/'), '', (SELECT path FROM system.disks WHERE name = 'default')),
    user_files_path, '/', currentDatabase(), '/05366_parquet_min_max_from_statistics_remote/') AS path
FROM system.server_settings WHERE name = 'user_files_path';

CREATE TABLE t_iceberg (i Int64)
ENGINE = IcebergLocal((SELECT path FROM iceberg_path), 'Parquet')
SETTINGS iceberg_format_version = 2, allow_experimental_iceberg_compaction = 0;
INSERT INTO t_iceberg SELECT number FROM numbers(1000);
DELETE FROM t_iceberg WHERE i IN (0, 999);

SET optimize_min_max_from_files = 1;

-- Read the data. The files read here are read again below only through statistics, so the count cache check at the
-- end sees what those reads stored.
SELECT min(str), max(str) FROM s3(s3_conn, filename = '05366/' || currentDatabase() || '_1.parquet') SETTINGS log_comment = '05366-s3_string';
SELECT min(i), max(i) FROM s3(s3_conn, filename = '05366/' || currentDatabase() || '_1.parquet') WHERE s > 0 SETTINGS log_comment = '05366-s3_where';
SELECT min(i), max(_path) LIKE '%05366%' FROM s3(s3_conn, filename = '05366/' || currentDatabase() || '_1.parquet') SETTINGS log_comment = '05366-s3_virtual_column';
SELECT min(p), max(p) FROM s3(s3_conn, filename = '05366/' || currentDatabase() || '_hive/*/*.parquet') SETTINGS use_hive_partitioning = 1, log_comment = '05366-s3_hive_partition_column';
SELECT min(i), max(i) FROM s3(s3_conn, filename = '05366/' || currentDatabase() || '_1.parquet') SETTINGS optimize_min_max_from_files = 0, log_comment = '05366-s3_disabled';
SELECT min(i), max(i) FROM url('http://localhost:11111/test/05366/' || currentDatabase() || '_1.parquet', Parquet) WHERE s > 0 SETTINGS log_comment = '05366-url_where';
SELECT min(i), max(i) FROM t_s3_policy SETTINGS log_comment = '05366-s3_row_policy';
SELECT min(i), max(i) FROM t_url_policy SETTINGS log_comment = '05366-url_row_policy';
SELECT min(id), max(id) FROM icebergS3(s3_conn, filename = 'deletes_db/eq_deletes_table') SETTINGS log_comment = '05366-iceberg_deletes';
SELECT min(i), max(i) FROM t_iceberg SETTINGS log_comment = '05366-iceberg_position_deletes';

-- Answered from the statistics.
SELECT min(i), max(i), min(s), max(s), min(d), max(d), min(dec), max(dec) FROM s3(s3_conn, filename = '05366/' || currentDatabase() || '_1.parquet') SETTINGS log_comment = '05366-s3';
SELECT min(n), max(n) FROM s3(s3_conn, filename = '05366/' || currentDatabase() || '_1.parquet') SETTINGS log_comment = '05366-s3_nullable';
SELECT min(i), max(i) FROM s3(s3_conn, filename = '05366/' || currentDatabase() || '_{1,2}.parquet') SETTINGS log_comment = '05366-s3_glob';
SELECT min(i), max(i) FROM s3(s3_conn, filename = '05366/' || currentDatabase() || '_hive/*/*.parquet') SETTINGS use_hive_partitioning = 1, log_comment = '05366-s3_hive_file_column';
SELECT min(i), max(i), min(d), max(d) FROM url('http://localhost:11111/test/05366/' || currentDatabase() || '_1.parquet', Parquet) SETTINGS log_comment = '05366-url';
SELECT min(i), max(i), min(d), max(d) FROM azureBlobStorage('DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://localhost:10000/devstoreaccount1;', 'cont05366', currentDatabase() || '_1.parquet', 'Parquet') SETTINGS log_comment = '05366-azure';
SELECT min(i), max(i) FROM s3Cluster('test_cluster_two_shards_localhost', s3_conn, filename = '05366/' || currentDatabase() || '_{1,2}.parquet') SETTINGS log_comment = '05366-s3_cluster';
SELECT min(i), max(i) FROM s3Cluster('test_cluster_two_shards_localhost', s3_conn, filename = '05366/' || currentDatabase() || '_1.parquet') SETTINGS cluster_table_function_split_granularity = 'bucket', log_comment = '05366-s3_cluster_bucket';

-- The two rows built from the statistics of a file are not its row count: every file above was last read through
-- statistics, so a cached count would be 2.
SELECT countIf(number_of_rows = 2) FROM system.schema_inference_cache WHERE position(source, '05366/' || currentDatabase() || '_') > 0;
SELECT count() FROM s3(s3_conn, filename = '05366/' || currentDatabase() || '_2.parquet') SETTINGS log_comment = '05366-s3_count';
SELECT count() FROM url('http://localhost:11111/test/05366/' || currentDatabase() || '_1.parquet', Parquet) SETTINGS log_comment = '05366-url_count';

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['ParquetReadMinMaxFromStatistics']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND is_initial_query AND log_comment LIKE '05366-%'
ORDER BY event_time_microseconds;

-- Each file (or row group bucket) read by the `s3Cluster` workers is answered from the statistics.
SELECT sum(ProfileEvents['ParquetReadMinMaxFromStatistics'])
FROM system.query_log
WHERE type = 'QueryFinish' AND NOT is_initial_query AND initial_query_id IN (
    SELECT query_id FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05366-s3_cluster');
SELECT sum(ProfileEvents['ParquetReadMinMaxFromStatistics'])
FROM system.query_log
WHERE type = 'QueryFinish' AND NOT is_initial_query AND initial_query_id IN (
    SELECT query_id FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05366-s3_cluster_bucket');

DROP ROW POLICY p_05366_s3 ON t_s3_policy;
DROP ROW POLICY p_05366_url ON t_url_policy;
DROP TABLE t_s3_policy;
DROP TABLE t_url_policy;
SET iceberg_delete_data_on_drop = 1;
DROP TABLE t_iceberg SYNC;
DROP TABLE iceberg_path;
