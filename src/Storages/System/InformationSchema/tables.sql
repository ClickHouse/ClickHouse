ATTACH VIEW tables
(
    `table_catalog` String,
    `table_schema` String,
    `table_name` String,
    `table_type` Enum8('BASE TABLE' = 1, 'VIEW' = 2, 'FOREIGN TABLE' = 3, 'LOCAL TEMPORARY' = 4, 'SYSTEM VIEW' = 5),
    `TABLE_CATALOG` String ALIAS table_catalog,
    `TABLE_SCHEMA` String ALIAS table_schema,
    `TABLE_NAME` String ALIAS table_name,
    `TABLE_TYPE` Enum8('BASE TABLE' = 1, 'VIEW' = 2, 'FOREIGN TABLE' = 3, 'LOCAL TEMPORARY' = 4, 'SYSTEM VIEW' = 5) ALIAS table_type
) AS
SELECT
    database AS table_catalog,
    database AS table_schema,
    name AS table_name,
    multiIf(is_temporary, 4, engine like '%View', 2, engine LIKE 'System%', 5, has_own_data = 0, 3, 1) AS table_type
FROM system.tables
