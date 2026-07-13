-- Tags: no-fasttest
-- Tag no-fasttest: Depends on AWS

SELECT * FROM icebergS3(s3_conn, filename='merged_several_tables_test', SETTINGS iceberg_metadata_table_uuid = 'ea8d1178-7756-4b89-b21f-00e9f31fe03e') ORDER BY id;
SELECT * FROM icebergS3(s3_conn, filename='merged_several_tables_test', SETTINGS iceberg_metadata_table_uuid = 'A90EED4CF74B4E5BB630096FB9D09021') ORDER BY id;
SELECT * FROM icebergS3(s3_conn, filename='merged_several_tables_test', SETTINGS iceberg_metadata_table_uuid = '6f6f6407_c6A5465f_A808ea8900_e35a38') ORDER BY id;
SELECT * FROM icebergS3(s3_conn, filename='merged_several_tables_test', SETTINGS iceberg_metadata_table_uuid = '88005553-5352-8222-8993-abacaba01010') ORDER BY id; -- { serverError FILE_DOESNT_EXIST }

SELECT count() FROM icebergS3(s3_conn, filename='merged_several_tables_test', SETTINGS iceberg_metadata_file_path = 'metadata/00001-aec4e034-3f73-48f7-87ad-51b7b42a8db7.metadata.json');
SELECT count() FROM icebergS3(s3_conn, filename='merged_several_tables_test', SETTINGS iceberg_metadata_file_path = 'metadata/00001-2aad93a8-a893-4943-8504-f6021f83ecab.metadata.json');
SELECT count() FROM icebergS3(s3_conn, filename='merged_several_tables_test', SETTINGS iceberg_metadata_file_path = 'metadata/00001-aec4e034-3f73-48f7-87ad-51b7b42a8db7.metadata.json');


SELECT * FROM icebergS3(s3_conn, filename='merged_several_tables_test', SETTINGS iceberg_recent_metadata_file_by_last_updated_ms_field = true) ORDER BY id;