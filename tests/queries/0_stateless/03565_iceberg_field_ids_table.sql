-- Tags: no-fasttest
-- Tag no-fasttest: Depends on AWS

SELECT * FROM icebergS3(s3_conn, filename='field_ids_table_test', SETTINGS iceberg_metadata_table_uuid = 'a5920f9f-12d0-4773-87fc-569c00676258') ORDER BY ALL;
