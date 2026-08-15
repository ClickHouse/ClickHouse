DROP VIEW IF EXISTS uuid2_table_function_schema_file;
DROP VIEW IF EXISTS uuid2_table_function_schema_file_cluster;
DROP VIEW IF EXISTS uuid2_table_function_schema_mongodb;
DROP VIEW IF EXISTS uuid2_table_function_schema_named;

SET uuid_type_version = 2;

CREATE VIEW uuid2_table_function_schema_file AS
    SELECT * FROM file('uuid2_table_function_schema.csv', 'CSV', 'id UUID');
CREATE VIEW uuid2_table_function_schema_file_cluster AS
    SELECT * FROM fileCluster('test_shard_localhost', 'uuid2_table_function_schema.csv', 'CSV', 'id UUID');
CREATE VIEW uuid2_table_function_schema_mongodb AS
    SELECT * FROM mongodb('mongodb://localhost/default', 'collection', 'id UUID');
CREATE VIEW uuid2_table_function_schema_named AS
    SELECT * FROM s3('https://example.com/uuid2_table_function_schema.csv', format = 'CSV', structure = 'id UUID');

SELECT name, position(as_select, 'UUID2') > 0
FROM system.tables
WHERE database = currentDatabase() AND name LIKE 'uuid2_table_function_schema_%'
ORDER BY name;

DROP VIEW uuid2_table_function_schema_file;
DROP VIEW uuid2_table_function_schema_file_cluster;
DROP VIEW uuid2_table_function_schema_mongodb;
DROP VIEW uuid2_table_function_schema_named;
