-- Tags: no-parallel, no-fasttest
-- Tag no-fasttest: Depends on AWS

SET s3_truncate_on_insert = 1;
SET s3_skip_empty_files = 0;

INSERT INTO FUNCTION s3(s3_conn, filename='dir1/03271_s3_table_function_asterisk_glob/', format=Parquet) SELECT 0 as num;
INSERT INTO FUNCTION s3(s3_conn, filename='dir1/03271_s3_table_function_asterisk_glob/file1', format=Parquet) SELECT 1 as num;
INSERT INTO FUNCTION s3(s3_conn, filename='dir1/03271_s3_table_function_asterisk_glob/file2', format=Parquet) SELECT 2 as num;
INSERT INTO FUNCTION s3(s3_conn, filename='dir1/03271_s3_table_function_asterisk_glob/file3', format=Parquet) SELECT 3 as num;

SELECT * FROM s3(s3_conn, filename='dir1/03271_s3_table_function_asterisk_glob/*') ORDER BY ALL SETTINGS max_threads = 1;
SELECT * FROM s3(s3_conn, filename='dir1/03271_s3_table_function_asterisk_glob/*') ORDER BY ALL SETTINGS max_threads = 4;

SELECT * FROM s3Cluster('test_cluster_two_shards_localhost', s3_conn, filename='dir1/03271_s3_table_function_asterisk_glob/*') ORDER BY ALL SETTINGS max_threads = 1;
SELECT * FROM s3Cluster('test_cluster_two_shards_localhost', s3_conn, filename='dir1/03271_s3_table_function_asterisk_glob/*') ORDER BY ALL SETTINGS max_threads = 4;

-- Empty "directory" files created implicitly by S3 console:
-- https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-folders.html
SELECT *
FROM s3('https://clickhouse-public-datasets.s3.amazonaws.com/wikistat/original/*', NOSIGN)
LIMIT 1
FORMAT Null;

SELECT *
FROM s3Cluster('test_cluster_two_shards_localhost', 'https://clickhouse-public-datasets.s3.amazonaws.com/wikistat/original/*', NOSIGN)
LIMIT 1
Format Null;
