-- Tags: no-fasttest
-- Tag no-fasttest: Depends on S3

INSERT INTO FUNCTION s3(s3_conn, url = 'http://localhost:11111/test/03762_data/file_'||currentDatabase()||'.parquet', format = Parquet) SETTINGS s3_truncate_on_insert = 1 SELECT number FROM numbers(10);
SYSTEM FLUSH LOGS blob_storage_log;
SELECT elapsed_microseconds > 0 FROM system.blob_storage_log WHERE remote_path = '03762_data/file_'||currentDatabase()||'.parquet' ORDER BY event_time DESC LIMIT 1;
