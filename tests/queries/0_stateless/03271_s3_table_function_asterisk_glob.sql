-- Tags: no-parallel, no-fasttest, long, no-flaky-check
-- Tag no-fasttest: Depends on AWS

SET send_logs_level = 'fatal';
SET s3_truncate_on_insert = 1;
-- The 0-byte directory markers match the globs below and cannot be parsed as Parquet
SET s3_skip_empty_files = 1;

-- An empty "directory" object as created implicitly by the S3 console:
-- https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-folders.html
-- The marker must stay 0-byte: SeaweedFS stores a key ending with '/' as a
-- directory and silently drops any content written into it.
INSERT INTO FUNCTION s3(s3_conn, filename='dir1/03271_s3_table_function_asterisk_glob/', format=RawBLOB, structure='x String') SELECT '';
INSERT INTO FUNCTION s3(s3_conn, filename='dir1/03271_s3_table_function_asterisk_glob/file1', format=Parquet) SELECT 1 as num;
INSERT INTO FUNCTION s3(s3_conn, filename='dir1/03271_s3_table_function_asterisk_glob/file2', format=Parquet) SELECT 2 as num;
INSERT INTO FUNCTION s3(s3_conn, filename='dir1/03271_s3_table_function_asterisk_glob/file3', format=Parquet) SELECT 3 as num;

SELECT * FROM s3(s3_conn, filename='dir1/03271_s3_table_function_asterisk_glob/*') ORDER BY ALL SETTINGS max_threads = 1;
SELECT * FROM s3(s3_conn, filename='dir1/03271_s3_table_function_asterisk_glob/*') ORDER BY ALL SETTINGS max_threads = 4;

SELECT * FROM s3Cluster('test_cluster_two_shards_localhost', s3_conn, filename='dir1/03271_s3_table_function_asterisk_glob/*') ORDER BY ALL SETTINGS max_threads = 1;
SELECT * FROM s3Cluster('test_cluster_two_shards_localhost', s3_conn, filename='dir1/03271_s3_table_function_asterisk_glob/*') ORDER BY ALL SETTINGS max_threads = 4;

-- The wikistat dataset also contains 0-byte directory markers that would fail
-- format detection when not skipped.
SELECT *
FROM s3('https://clickhouse-public-datasets.s3.amazonaws.com/wikistat/original/*', NOSIGN)
LIMIT 1
FORMAT Null;

SELECT *
FROM s3Cluster('test_cluster_two_shards_localhost', 'https://clickhouse-public-datasets.s3.amazonaws.com/wikistat/original/*', NOSIGN)
LIMIT 1
Format Null;
