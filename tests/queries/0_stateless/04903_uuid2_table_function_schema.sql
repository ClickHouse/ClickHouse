DROP VIEW IF EXISTS uuid2_table_function_schema_file;
DROP VIEW IF EXISTS uuid2_table_function_schema_file_cluster;
DROP VIEW IF EXISTS uuid2_table_function_schema_format;
DROP VIEW IF EXISTS uuid2_table_function_schema_s3_credentials;

SET uuid_type_version = 2;

CREATE VIEW uuid2_table_function_schema_file AS
    SELECT * FROM file('uuid2_table_function_schema.csv', 'CSV', 'id UUID');
CREATE VIEW uuid2_table_function_schema_file_cluster AS
    SELECT * FROM fileCluster('test_shard_localhost', 'uuid2_table_function_schema.csv', 'CSV', 'id UUID');
CREATE VIEW uuid2_table_function_schema_format AS
    SELECT * FROM format('CSV', 'id UUID', '00000000-0000-0000-0000-000000000000');
CREATE VIEW uuid2_table_function_schema_s3_credentials AS
    SELECT * FROM s3('https://example.com/data.csv', 'access_key', 'secret_key', 'CSV', 'id UUID');

SELECT name, position(as_select, 'UUID2') > 0
FROM system.tables
WHERE database = currentDatabase() AND name LIKE 'uuid2_table_function_schema_%'
ORDER BY name;

DROP VIEW uuid2_table_function_schema_file;
DROP VIEW uuid2_table_function_schema_file_cluster;
DROP VIEW uuid2_table_function_schema_format;
DROP VIEW uuid2_table_function_schema_s3_credentials;
