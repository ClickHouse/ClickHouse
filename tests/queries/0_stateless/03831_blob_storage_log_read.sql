-- Tags: no-fasttest
-- Tag no-fasttest: Depends on S3

SET enable_blob_storage_log_for_read_operations = 1;

INSERT INTO FUNCTION s3(s3_conn, url = 'http://localhost:11111/test/03831_data/file_'||currentDatabase()||'.csv', format = CSV)
    SETTINGS s3_truncate_on_insert = 1
    SELECT number FROM numbers(100);

SELECT sum(number) FROM s3(s3_conn, url = 'http://localhost:11111/test/03831_data/file_'||currentDatabase()||'.csv', format = CSV);

SYSTEM FLUSH LOGS blob_storage_log;

SELECT count() > 0, countIf(error_code = 0) = count()
FROM system.blob_storage_log
WHERE event_type = 'Read'
    AND remote_path LIKE '%03831_data/file_%'
    AND event_date >= yesterday();
