ATTACH VIEW tables
(
    `table_catalog` String,
    `table_schema` String,
    `table_name` String,
    `table_type` String,
    `table_collation` Nullable(String),
    `table_comment` Nullable(String),
    `TABLE_CATALOG` String ALIAS table_catalog,
    `TABLE_SCHEMA` String ALIAS table_schema,
    `TABLE_NAME` String ALIAS table_name,
    `TABLE_TYPE` String ALIAS table_type,
    `TABLE_COLLATION` Nullable(String) ALIAS table_collation,
    `TABLE_COMMENT` Nullable(String) ALIAS table_comment
) AS
SELECT
    database  AS table_catalog,
    database  AS table_schema,
    name      AS table_name,
    multiIf(is_temporary,          'LOCAL TEMPORARY', 
            engine LIKE '%View',   'VIEW', 
            engine LIKE 'System%', 'SYSTEM VIEW', 
            has_own_data = 0,      'FOREIGN TABLE', 
            'BASE TABLE'
            ) AS table_type,
    'utf8mb4' AS table_collation,
    comment   AS table_comment
FROM system.tables
