-- Tags: no-fasttest, no-replicated-database
-- Tag no-fasttest: Parquet, S3 and Iceberg
-- Tag no-replicated-database: the replicated DDL worker evaluates the `IcebergLocal` arguments without the session, so it does not see the `TEMPORARY TABLE iceberg_paths`

-- File, URL and object storage reads give whole files to their streams. With at least as many files as threads,
-- each stream keeps its own output (`AffinityResize`), and an output whose stream has run out of files takes
-- chunks of the other streams. `enable_parallel_replicas` would replace the reads with cluster reads.

SET max_threads = 2, max_threads_min_free_memory_per_thread = 0, max_streams_to_max_threads_ratio = 1, enable_parallel_replicas = 0;
SET input_format_parquet_preserve_order = 0, parallelize_output_from_storages = 1;
SET engine_file_truncate_on_insert = 1, s3_truncate_on_insert = 1;

INSERT INTO FUNCTION file(currentDatabase() || '_05361_0.parquet') SELECT number AS k FROM numbers(100000);
INSERT INTO FUNCTION file(currentDatabase() || '_05361_1.parquet') SELECT number AS k FROM numbers(10);
INSERT INTO FUNCTION file(currentDatabase() || '_05361_2.parquet') SELECT number AS k FROM numbers(10);
INSERT INTO FUNCTION file(currentDatabase() || '_05361_3.parquet') SELECT number AS k FROM numbers(10);
INSERT INTO FUNCTION s3(s3_conn, filename = currentDatabase() || '_05361/data_0.parquet') SELECT number AS k FROM numbers(100000);
INSERT INTO FUNCTION s3(s3_conn, filename = currentDatabase() || '_05361/data_1.parquet') SELECT number AS k FROM numbers(10);
INSERT INTO FUNCTION s3(s3_conn, filename = currentDatabase() || '_05361/data_2.parquet') SELECT number AS k FROM numbers(10);
INSERT INTO FUNCTION s3(s3_conn, filename = currentDatabase() || '_05361/data_3.parquet') SELECT number AS k FROM numbers(10);

SELECT 'pipeline';
SELECT countIf(trimLeft(explain) = 'AffinityResize 2 → 2') FROM (EXPLAIN PIPELINE SELECT k % 7 AS g, count() FROM file(currentDatabase() || '_05361_*.parquet') GROUP BY g);
SELECT countIf(trimLeft(explain) = 'AffinityResize 2 → 2') FROM (EXPLAIN PIPELINE SELECT k % 7 AS g, count() FROM s3(s3_conn, filename = currentDatabase() || '_05361/data_*.parquet') GROUP BY g);
SELECT countIf(trimLeft(explain) = 'AffinityResize 2 → 2') FROM (EXPLAIN PIPELINE SELECT k % 7 AS g, count() FROM url('http://localhost:11111/test/' || currentDatabase() || '_05361/data_{0..3}.parquet', Parquet, 'k UInt64') GROUP BY g);
-- A single file is resized to `max_threads`.
SELECT countIf(trimLeft(explain) LIKE 'AffinityResize%'), countIf(trimLeft(explain) = 'Resize 1 → 2') FROM (EXPLAIN PIPELINE SELECT k FROM file(currentDatabase() || '_05361_0.parquet'));
-- With many streams the streams are split into groups of `min_outstreams_per_resize_after_split`.
SELECT countIf(trimLeft(explain) = 'AffinityResize × 2 2 → 2') FROM (EXPLAIN PIPELINE SELECT k % 7 AS g, count() FROM file(currentDatabase() || '_05361_*.parquet') GROUP BY g) SETTINGS max_threads = 4, min_outstreams_per_resize_after_split = 2;
SELECT countIf(trimLeft(explain) = 'AffinityResize 4 → 4') FROM (EXPLAIN PIPELINE SELECT k % 7 AS g, count() FROM file(currentDatabase() || '_05361_*.parquet') GROUP BY g) SETTINGS max_threads = 4, min_outstreams_per_resize_after_split = 0;
-- Reading in order and `parallelize_output_from_storages = 0` keep the streams as they are.
SET input_format_parquet_preserve_order = 1;
SELECT countIf(trimLeft(explain) LIKE 'AffinityResize%') FROM (EXPLAIN PIPELINE SELECT k % 7 AS g, count() FROM file(currentDatabase() || '_05361_*.parquet') GROUP BY g);
SET input_format_parquet_preserve_order = 0, parallelize_output_from_storages = 0;
SELECT countIf(trimLeft(explain) LIKE 'AffinityResize%') FROM (EXPLAIN PIPELINE SELECT k % 7 AS g, count() FROM file(currentDatabase() || '_05361_*.parquet') GROUP BY g);
SET parallelize_output_from_storages = 1;
-- So does `input_format_parquet_preserve_order` in the `SETTINGS` of a table, which its reader uses instead of the query setting.
CREATE TABLE t_preserve_order (k UInt64) ENGINE = File(Parquet) SETTINGS input_format_parquet_preserve_order = 1;
INSERT INTO t_preserve_order SELECT number FROM numbers(1000) SETTINGS engine_file_truncate_on_insert = 0, engine_file_allow_create_multiple_files = 1;
INSERT INTO t_preserve_order SELECT number FROM numbers(10) SETTINGS engine_file_truncate_on_insert = 0, engine_file_allow_create_multiple_files = 1;
INSERT INTO t_preserve_order SELECT number FROM numbers(10) SETTINGS engine_file_truncate_on_insert = 0, engine_file_allow_create_multiple_files = 1;
SELECT countIf(trimLeft(explain) LIKE '%Resize%') FROM (EXPLAIN PIPELINE SELECT k FROM t_preserve_order);
SELECT countIf(trimLeft(explain) LIKE '%Resize%') FROM (EXPLAIN PIPELINE SELECT k FROM t_preserve_order) SETTINGS max_threads = 8;
CREATE TABLE t_preserve_order_s3 (k UInt64) ENGINE = S3(s3_conn, filename = currentDatabase() || '_05361/data_*.parquet', format = Parquet) SETTINGS input_format_parquet_preserve_order = 1;
SELECT countIf(trimLeft(explain) LIKE '%Resize%') FROM (EXPLAIN PIPELINE SELECT k FROM t_preserve_order_s3);
SELECT countIf(trimLeft(explain) LIKE '%Resize%') FROM (EXPLAIN PIPELINE SELECT k FROM t_preserve_order_s3) SETTINGS max_threads = 8;
CREATE TABLE t_preserve_order_url (k UInt64) ENGINE = URL('http://localhost:11111/test/' || currentDatabase() || '_05361/data_{0..3}.parquet', Parquet) SETTINGS input_format_parquet_preserve_order = 1;
SELECT countIf(trimLeft(explain) LIKE '%Resize%') FROM (EXPLAIN PIPELINE SELECT k FROM t_preserve_order_url);
SELECT countIf(trimLeft(explain) LIKE '%Resize%') FROM (EXPLAIN PIPELINE SELECT k FROM t_preserve_order_url) SETTINGS max_threads = 8;
DROP TABLE t_preserve_order;
DROP TABLE t_preserve_order_s3;
DROP TABLE t_preserve_order_url;

SELECT 'file';
SELECT arraySort(groupArray((g, c, s))) FROM (SELECT k % 7 AS g, count() AS c, sum(k) AS s FROM file(currentDatabase() || '_05361_*.parquet') GROUP BY g);
SELECT arraySort(groupArray((g, c, s))) FROM (SELECT k % 7 AS g, count() AS c, sum(k) AS s FROM file(currentDatabase() || '_05361_*.parquet') GROUP BY g) SETTINGS max_threads = 3;
SELECT arraySort(groupArray((g, c, s))) FROM (SELECT k % 7 AS g, count() AS c, sum(k) AS s FROM file(currentDatabase() || '_05361_*.parquet') GROUP BY g) SETTINGS max_threads = 4;
SELECT arraySort(groupArray((g, c, s))) FROM (SELECT k % 7 AS g, count() AS c, sum(k) AS s FROM file(currentDatabase() || '_05361_*.parquet') GROUP BY g) SETTINGS max_threads = 4, min_outstreams_per_resize_after_split = 2;
SELECT arraySort(groupArray((g, c, s))) FROM (SELECT k % 7 AS g, count() AS c, sum(k) AS s FROM file(currentDatabase() || '_05361_*.parquet') GROUP BY g) SETTINGS parallelize_output_from_storages = 0;
SELECT count() FROM (SELECT k FROM file(currentDatabase() || '_05361_*.parquet') LIMIT 5);
SELECT count() FROM (SELECT k FROM file(currentDatabase() || '_05361_*.parquet') LIMIT 50000);

SELECT 's3';
SELECT arraySort(groupArray((g, c, s))) FROM (SELECT k % 7 AS g, count() AS c, sum(k) AS s FROM s3(s3_conn, filename = currentDatabase() || '_05361/data_*.parquet') GROUP BY g);
SELECT arraySort(groupArray((g, c, s))) FROM (SELECT k % 7 AS g, count() AS c, sum(k) AS s FROM s3(s3_conn, filename = currentDatabase() || '_05361/data_*.parquet') GROUP BY g) SETTINGS max_threads = 3;
SELECT arraySort(groupArray((g, c, s))) FROM (SELECT k % 7 AS g, count() AS c, sum(k) AS s FROM s3(s3_conn, filename = currentDatabase() || '_05361/data_*.parquet') GROUP BY g) SETTINGS max_threads = 4;
SELECT arraySort(groupArray((g, c, s))) FROM (SELECT k % 7 AS g, count() AS c, sum(k) AS s FROM s3(s3_conn, filename = currentDatabase() || '_05361/data_*.parquet') GROUP BY g) SETTINGS parallelize_output_from_storages = 0;
SELECT count() FROM (SELECT k FROM s3(s3_conn, filename = currentDatabase() || '_05361/data_*.parquet') LIMIT 5);
SELECT count() FROM (SELECT k FROM s3(s3_conn, filename = currentDatabase() || '_05361/data_*.parquet') LIMIT 50000);

SELECT 'url';
SELECT arraySort(groupArray((g, c, s))) FROM (SELECT k % 7 AS g, count() AS c, sum(k) AS s FROM url('http://localhost:11111/test/' || currentDatabase() || '_05361/data_{0..3}.parquet', Parquet, 'k UInt64') GROUP BY g);
SELECT arraySort(groupArray((g, c, s))) FROM (SELECT k % 7 AS g, count() AS c, sum(k) AS s FROM url('http://localhost:11111/test/' || currentDatabase() || '_05361/data_{0..3}.parquet', Parquet, 'k UInt64') GROUP BY g) SETTINGS max_threads = 3;
SELECT arraySort(groupArray((g, c, s))) FROM (SELECT k % 7 AS g, count() AS c, sum(k) AS s FROM url('http://localhost:11111/test/' || currentDatabase() || '_05361/data_{0..3}.parquet', Parquet, 'k UInt64') GROUP BY g) SETTINGS max_threads = 4;
SELECT arraySort(groupArray((g, c, s))) FROM (SELECT k % 7 AS g, count() AS c, sum(k) AS s FROM url('http://localhost:11111/test/' || currentDatabase() || '_05361/data_{0..3}.parquet', Parquet, 'k UInt64') GROUP BY g) SETTINGS parallelize_output_from_storages = 0;
SELECT count() FROM (SELECT k FROM url('http://localhost:11111/test/' || currentDatabase() || '_05361/data_{0..3}.parquet', Parquet, 'k UInt64') LIMIT 5);
SELECT count() FROM (SELECT k FROM url('http://localhost:11111/test/' || currentDatabase() || '_05361/data_{0..3}.parquet', Parquet, 'k UInt64') LIMIT 50000);

SELECT 'iceberg';
SET allow_insert_into_iceberg = 1, allow_experimental_iceberg_compaction = 0, use_iceberg_metadata_files_cache = 0, iceberg_delete_data_on_drop = 0, async_insert = 0;
CREATE TEMPORARY TABLE iceberg_paths AS
WITH if(changed, trimBoth(value), 'user_files/') AS user_files_path,
    concat(if(startsWith(user_files_path, '/'), '', (SELECT path FROM system.disks WHERE name = 'default')), user_files_path, '/', currentDatabase(), '/') AS dir
SELECT dir || '05361_sorted/' AS sorted, dir || '05361_unsorted/' AS unsorted, dir || '05361_preserve_order/' AS preserve_order
FROM system.server_settings WHERE name = 'user_files_path';
CREATE TABLE t_sorted (k Int64) ENGINE = IcebergLocal((SELECT sorted FROM iceberg_paths), 'Parquet') ORDER BY k;
CREATE TABLE t_unsorted (k Int64) ENGINE = IcebergLocal((SELECT unsorted FROM iceberg_paths), 'Parquet');
CREATE TABLE t_preserve_order_iceberg (k Int64) ENGINE = IcebergLocal((SELECT preserve_order FROM iceberg_paths), 'Parquet') SETTINGS input_format_parquet_preserve_order = 1;
INSERT INTO t_sorted SELECT number FROM numbers(1000);
INSERT INTO t_sorted SELECT number + 1000 FROM numbers(10);
INSERT INTO t_unsorted SELECT number FROM numbers(1000);
INSERT INTO t_unsorted SELECT number + 1000 FROM numbers(10);
INSERT INTO t_preserve_order_iceberg SELECT number FROM numbers(1000);
INSERT INTO t_preserve_order_iceberg SELECT number + 1000 FROM numbers(10);
-- A table with a sorting key may be read in order, so its streams are kept.
SELECT countIf(trimLeft(explain) = 'AffinityResize 2 → 2') FROM (EXPLAIN PIPELINE SELECT k % 7 AS g, count() FROM t_unsorted GROUP BY g);
SELECT countIf(trimLeft(explain) LIKE 'AffinityResize%') FROM (EXPLAIN PIPELINE SELECT k % 7 AS g, count() FROM t_sorted GROUP BY g);
SELECT countIf(trimLeft(explain) LIKE 'AffinityResize%') FROM (EXPLAIN PIPELINE SELECT k FROM t_sorted ORDER BY k);
SELECT k FROM t_sorted ORDER BY k LIMIT 3 OFFSET 998;
SELECT count(), sum(k) FROM t_unsorted;
-- So does a data lake table with `input_format_parquet_preserve_order` in its `SETTINGS`.
SELECT countIf(trimLeft(explain) LIKE '%Resize%') FROM (EXPLAIN PIPELINE SELECT k FROM t_preserve_order_iceberg);
SELECT countIf(trimLeft(explain) LIKE '%Resize%') FROM (EXPLAIN PIPELINE SELECT k FROM t_preserve_order_iceberg) SETTINGS max_threads = 8;
DROP TABLE t_sorted;
DROP TABLE t_unsorted;
DROP TABLE t_preserve_order_iceberg;
DROP TABLE iceberg_paths;
