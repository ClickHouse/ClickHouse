-- Tags: no-fasttest
-- Tag no-fasttest: Depends on AWS

SELECT * FROM icebergS3(s3_conn, filename='field_ids_table_test', SETTINGS iceberg_metadata_table_uuid = '8f1f9ae2-18bb-421e-b640-ec2f85e67bce') ORDER BY ALL;
