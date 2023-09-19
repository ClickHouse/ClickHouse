ATTACH VIEW tables
(
    `table_catalog` String,
    `table_schema` String,
    `table_name` String,
    `table_type` String,
    `table_collation` Nullable(String),
    `table_comment` Nullable(String),
    `TABLE_CATALOG` String,
    `TABLE_SCHEMA` String,
    `TABLE_NAME` String,
    `TABLE_TYPE` String,
    `TABLE_COLLATION` Nullable(String),
    `TABLE_COMMENT` Nullable(String)
) AS
SELECT
    database             AS table_catalog,
    database             AS table_schema,
    name                 AS table_name,
    multiIf(is_temporary,          'LOCAL TEMPORARY', 
            engine LIKE '%View',   'VIEW', 
            engine LIKE 'System%', 'SYSTEM VIEW', 
            has_own_data = 0,      'FOREIGN TABLE', 
            'BASE TABLE'
            )            AS table_type,
    'utf8mb4_0900_ai_ci' AS table_collation,
    comment              AS table_comment,
    table_catalog        AS TABLE_CATALOG,
    table_schema         AS TABLE_SCHEMA,
    table_name           AS TABLE_NAME,
    table_type           AS TABLE_TYPE,
    table_collation      AS TABLE_COLLATION,
    table_comment        AS TABLE_COMMENT
FROM system.tables
