-- Tags: no-fasttest
-- Tag no-fasttest: Depends on AWS

SELECT * FROM icebergS3(s3_conn, filename='field_ids_table_test', SETTINGS iceberg_metadata_table_uuid = 'd1effb24-3d84-4d18-ae2c-b6d3cacc574a') ORDER BY ALL;
