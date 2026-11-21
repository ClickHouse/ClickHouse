DROP VIEW IF EXISTS uuid2_table_function_schema_file;
DROP VIEW IF EXISTS uuid2_table_function_schema_file_cluster;
DROP VIEW IF EXISTS uuid2_table_function_schema_format;
DROP VIEW IF EXISTS uuid2_table_function_schema_values_data;
DROP VIEW IF EXISTS uuid2_table_function_schema_values_schema;

SET uuid_type_version = 2;

CREATE VIEW uuid2_table_function_schema_file AS
    SELECT * FROM file('uuid2_table_function_schema.csv', 'CSV', 'id UUID');
CREATE VIEW uuid2_table_function_schema_file_cluster AS
    SELECT * FROM fileCluster('test_shard_localhost', 'uuid2_table_function_schema.csv', 'CSV', 'id UUID');
CREATE VIEW uuid2_table_function_schema_format AS
    SELECT * FROM format('CSV', 'id UUID', '00000000-0000-0000-0000-000000000000');
CREATE VIEW uuid2_table_function_schema_values_data AS
    SELECT * FROM values('id UUID');
CREATE VIEW uuid2_table_function_schema_values_schema AS
    SELECT * FROM values('id UUID', '00000000-0000-0000-0000-000000000000');

SELECT name, position(as_select, 'UUID2') > 0
FROM system.tables
WHERE database = currentDatabase() AND name NOT LIKE 'uuid2_table_function_schema_values_%' AND name LIKE 'uuid2_table_function_schema_%'
ORDER BY name;

SELECT name, position(as_select, 'UUID2') > 0
FROM system.tables
WHERE database = currentDatabase() AND name LIKE 'uuid2_table_function_schema_values_%'
ORDER BY name;

DROP VIEW uuid2_table_function_schema_file, uuid2_table_function_schema_file_cluster, uuid2_table_function_schema_format;
DROP VIEW uuid2_table_function_schema_values_data, uuid2_table_function_schema_values_schema;
