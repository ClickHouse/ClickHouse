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
    `INDEX_LENGTH` Nullable(UInt64),
    `TABLE_COLLATION` Nullable(String),
    `TABLE_COMMENT` Nullable(String),
    `engine` Nullable(String),
    `version` Nullable(UInt64),
    `row_format` Nullable(String),
    `avg_row_length` Nullable(UInt64),
    `max_data_length` Nullable(UInt64),
    `data_free` Nullable(UInt64),
    `auto_increment` Nullable(UInt64),
    `create_time` Nullable(DateTime),
    `update_time` Nullable(DateTime),
    `check_time` Nullable(DateTime),
    `checksum` Nullable(Int64),
    `create_options` Nullable(String),
    `ENGINE` Nullable(String),
    `VERSION` Nullable(UInt64),
    `ROW_FORMAT` Nullable(String),
    `AVG_ROW_LENGTH` Nullable(UInt64),
    `MAX_DATA_LENGTH` Nullable(UInt64),
    `DATA_FREE` Nullable(UInt64),
    `AUTO_INCREMENT` Nullable(UInt64),
    `CREATE_TIME` Nullable(DateTime),
    `UPDATE_TIME` Nullable(DateTime),
    `CHECK_TIME` Nullable(DateTime),
    `CHECKSUM` Nullable(Int64),
    `CREATE_OPTIONS` Nullable(String)
)
SQL SECURITY INVOKER
AS SELECT
    database             AS table_catalog,
    database             AS table_schema,
    name                 AS table_name,
    multiIf(is_temporary,            'LOCAL TEMPORARY',
            t.engine LIKE '%View',   'VIEW',
            t.engine LIKE 'System%', 'SYSTEM VIEW',
            has_own_data = 0,        'FOREIGN TABLE',
            'BASE TABLE'
            )            AS table_type,
    total_rows           AS table_rows,
    total_bytes          AS data_length,
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
    index_length         AS INDEX_LENGTH,
    table_collation      AS TABLE_COLLATION,
    table_comment        AS TABLE_COMMENT,
    -- MySQL-compatibility columns, appended after the standard columns to preserve their ordinal positions
    -- In MySQL, ENGINE is a real storage engine and is NULL for views; suppress it only for
    -- view-like rows so MySQL-aware tools don't see internal names like 'View' or 'System...',
    -- while real table engines that don't store data on disk (e.g. Memory) keep their name
    if(table_type IN ('VIEW', 'SYSTEM VIEW'), NULL, t.engine)
                         AS engine,            -- MySQL-specific
    NULL                 AS version,           -- MySQL-specific
    NULL                 AS row_format,        -- MySQL-specific
    NULL                 AS avg_row_length,    -- MySQL-specific
    NULL                 AS max_data_length,   -- MySQL-specific
    NULL                 AS data_free,         -- MySQL-specific
    NULL                 AS auto_increment,    -- MySQL-specific
    NULL                 AS create_time,       -- MySQL-specific
    NULL                 AS update_time,       -- MySQL-specific
    NULL                 AS check_time,        -- MySQL-specific
    NULL                 AS checksum,          -- MySQL-specific
    NULL                 AS create_options,    -- MySQL-specific
    engine               AS ENGINE,
    version              AS VERSION,
    row_format           AS ROW_FORMAT,
    avg_row_length       AS AVG_ROW_LENGTH,
    max_data_length      AS MAX_DATA_LENGTH,
    data_free            AS DATA_FREE,
    auto_increment       AS AUTO_INCREMENT,
    create_time          AS CREATE_TIME,
    update_time          AS UPDATE_TIME,
    check_time           AS CHECK_TIME,
    checksum             AS CHECKSUM,
    create_options       AS CREATE_OPTIONS
FROM system.tables t
LEFT JOIN system.parts p ON (t.database = p.database AND t.name = p.table AND p.active = 1)
GROUP BY
    t.database,
    t.name,
    t.is_temporary,
    t.engine,
    t.has_own_data,
    t.total_rows,
    t.total_bytes,
    t.comment
