-- Tags: no-fasttest
-- Tag no-fasttest: Depends on AWS

SELECT * FROM icebergS3Cluster('default', 'http://localhost:11111/test/est', 'clickhouse', 'clickhouse', SETTINGS iceberg_metadata_file_path = 'metadata/v2.metadata.json');
SELECT * FROM icebergS3Cluster('default', s3_conn, filename='est', SETTINGS iceberg_metadata_file_path = 'metadata/v2.metadata.json');