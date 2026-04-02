ATTACH VIEW tables
(
    `table_catalog` String,
    `table_schema` String,
    `table_name` String,
    `table_type` String,
    `table_rows` Nullable(UInt64),
    `data_length` Nullable(UInt64),
    `index_length` Nullable(UInt64),
    `table_collation` Nullable(String),
    `table_comment` Nullable(String),
    `TABLE_CATALOG` String,
    `TABLE_SCHEMA` String,
    `TABLE_NAME` String,
    `TABLE_TYPE` String,
    `TABLE_ROWS` Nullable(UInt64),
    `DATA_LENGTH` Nullable(UInt64),
    `TABLE_COLLATION` Nullable(String),
    `TABLE_COMMENT` Nullable(String)
)
SQL SECURITY INVOKER
AS SELECT
    database             AS table_catalog,
    database             AS table_schema,
    name                 AS table_name,
    multiIf(is_temporary,          'LOCAL TEMPORARY',
            engine LIKE '%View',   'VIEW',
            engine LIKE 'System%', 'SYSTEM VIEW',
            has_own_data = 0,      'FOREIGN TABLE',
            'BASE TABLE'
            )            AS table_type,
    total_rows AS table_rows,
    total_bytes AS data_length,
    sum(p.primary_key_size + p.marks_bytes
        + p.secondary_indices_compressed_bytes + p.secondary_indices_marks_bytes
    ) AS index_length,
    'utf8mb4_0900_ai_ci' AS table_collation,
    comment              AS table_comment,
    table_catalog        AS TABLE_CATALOG,
    table_schema         AS TABLE_SCHEMA,
    table_name           AS TABLE_NAME,
    table_type           AS TABLE_TYPE,
    table_rows           AS TABLE_ROWS,
    data_length          AS DATA_LENGTH,
    table_collation      AS TABLE_COLLATION,
    table_comment        AS TABLE_COMMENT
FROM system.tables t
LEFT JOIN system.parts p ON (t.database = p.database AND t.name = p.table)
GROUP BY
    t.database,
    t.name,
    t.is_temporary,
    t.engine,
    t.has_own_data,
    t.total_rows,
    t.total_bytes,
    t.comment
