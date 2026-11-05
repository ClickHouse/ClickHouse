-- Tags: no-fasttest
-- Tag no-fasttest: Depends on S3

SET enable_blob_storage_log_for_read_operations = 1;

INSERT INTO FUNCTION s3(s3_conn, url = 'http://localhost:11111/test/04039_data/file_'||currentDatabase()||'.csv', structure = 'number UInt64', format = CSV)
    SETTINGS s3_truncate_on_insert = 1
    SELECT number FROM numbers(100);

SELECT sum(number) FROM s3(s3_conn, url = 'http://localhost:11111/test/04039_data/file_'||currentDatabase()||'.csv', structure = 'number UInt64', format = CSV);

SYSTEM FLUSH LOGS blob_storage_log;

SELECT count() > 0, countIf(error_code = 0) = count(), countIf(elapsed_microseconds > 0) > 0, countIf(data_size > 0) > 0
FROM system.blob_storage_log
WHERE event_type = 'Read'
    AND remote_path LIKE '%04039_data/file_' || currentDatabase() || '.csv'
    AND event_date >= yesterday()
    AND event_time > now() - INTERVAL 5 MINUTE;

-- Negative test: with enable_blob_storage_log_for_read_operations=0 (default), no Read events should be logged.
-- Use a different file name to distinguish from the positive test above.
INSERT INTO FUNCTION s3(s3_conn, url = 'http://localhost:11111/test/04039_data/neg_'||currentDatabase()||'.csv', structure = 'number UInt64', format = CSV)
    SETTINGS s3_truncate_on_insert = 1
    SELECT number FROM numbers(10);

SELECT sum(number) FROM s3(s3_conn, url = 'http://localhost:11111/test/04039_data/neg_'||currentDatabase()||'.csv', structure = 'number UInt64', format = CSV)
    SETTINGS enable_blob_storage_log_for_read_operations = 0;

SYSTEM FLUSH LOGS blob_storage_log;

SELECT countIf(event_type = 'Read') = 0
FROM system.blob_storage_log
WHERE remote_path LIKE '%04039_data/neg_' || currentDatabase() || '.csv'
    AND event_date >= yesterday()
    AND event_time > now() - INTERVAL 5 MINUTE;

-- Gate test: enable_blob_storage_log_for_read_operations=1 but enable_blob_storage_log=0 must not produce any Read events,
-- because the blob storage log writer is gated by the parent setting.
INSERT INTO FUNCTION s3(s3_conn, url = 'http://localhost:11111/test/04039_data/gate_'||currentDatabase()||'.csv', structure = 'number UInt64', format = CSV)
    SETTINGS s3_truncate_on_insert = 1
    SELECT number FROM numbers(10);

SELECT sum(number) FROM s3(s3_conn, url = 'http://localhost:11111/test/04039_data/gate_'||currentDatabase()||'.csv', structure = 'number UInt64', format = CSV)
    SETTINGS enable_blob_storage_log = 0, enable_blob_storage_log_for_read_operations = 1;

SYSTEM FLUSH LOGS blob_storage_log;

SELECT countIf(event_type = 'Read') = 0
FROM system.blob_storage_log
WHERE remote_path LIKE '%04039_data/gate_' || currentDatabase() || '.csv'
    AND event_date >= yesterday()
    AND event_time > now() - INTERVAL 5 MINUTE;
