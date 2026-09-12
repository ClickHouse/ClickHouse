-- Tags: no-fasttest
-- Tag no-fasttest: Depends on AWS

-- `iceberg_metadata_table_uuid` selects the latest metadata file by filtering the table's
-- `metadata/` directory by `table-uuid`, so the selected file is only valid for that exact
-- (table path, UUID) pair. The latest-metadata-version cache is keyed by the table path or by the
-- UUID alone, so a warm entry must not be reused across different `iceberg_metadata_table_uuid`
-- values for the same path (nor across different paths for the same UUID).

SET use_iceberg_metadata_files_cache = 1;
SET iceberg_metadata_staleness_ms = 600000;

-- Warm the cache for the first table sharing the directory.
SELECT * FROM icebergS3(s3_conn, filename='merged_several_tables_test', SETTINGS iceberg_metadata_table_uuid = 'ea8d1178-7756-4b89-b21f-00e9f31fe03e') ORDER BY id;

-- Different UUIDs in the same directory must still select their own metadata file.
SELECT * FROM icebergS3(s3_conn, filename='merged_several_tables_test', SETTINGS iceberg_metadata_table_uuid = 'A90EED4CF74B4E5BB630096FB9D09021') ORDER BY id;
SELECT * FROM icebergS3(s3_conn, filename='merged_several_tables_test', SETTINGS iceberg_metadata_table_uuid = '6f6f6407_c6A5465f_A808ea8900_e35a38') ORDER BY id;

-- An unknown UUID must fail rather than reuse another UUID's cached selection.
SELECT * FROM icebergS3(s3_conn, filename='merged_several_tables_test', SETTINGS iceberg_metadata_table_uuid = '88005553-5352-8222-8993-abacaba01010') ORDER BY id; -- { serverError FILE_DOESNT_EXIST }

-- Repeating the first query with the cache warm must not pick up another UUID's selection either.
SELECT * FROM icebergS3(s3_conn, filename='merged_several_tables_test', SETTINGS iceberg_metadata_table_uuid = 'ea8d1178-7756-4b89-b21f-00e9f31fe03e') ORDER BY id;
