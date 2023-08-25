ATTACH VIEW key_column_usage
    (
     `referenced_table_schema` Nullable(String),
     `referenced_table_name` Nullable(String),
     `referenced_column_name` Nullable(String),
     `table_schema` String,
     `table_name` String,
     `column_name` Nullable(String),
     `ordinal_position` UInt32,
     `constraint_name` Nullable(String),
     `REFERENCED_TABLE_SCHEMA` Nullable(String),
     `REFERENCED_TABLE_NAME` Nullable(String),
     `REFERENCED_COLUMN_NAME` Nullable(String),
     `TABLE_SCHEMA` String,
     `TABLE_NAME` String,
     `COLUMN_NAME` Nullable(String),
     `ORDINAL_POSITION` UInt32,
     `CONSTRAINT_NAME` Nullable(String)
        ) AS
SELECT NULL                      AS `referenced_table_schema`,
       NULL                      AS `referenced_table_name`,
       NULL                      AS `referenced_column_name`,
       database                  AS `table_schema`,
       table                     AS `table_name`,
       name                      AS `column_name`,
       position                  AS `ordinal_position`,
       'PRIMARY'                 AS `constraint_name`,

       `referenced_table_schema` AS `REFERENCED_TABLE_SCHEMA`,
       `referenced_table_name`   AS `REFERENCED_TABLE_NAME`,
       `referenced_column_name`  AS `REFERENCED_COLUMN_NAME`,
       `table_schema`            AS `TABLE_SCHEMA`,
       `table_name`              AS `TABLE_NAME`,
       `column_name`             AS `COLUMN_NAME`,
       `ordinal_position`        AS `ORDINAL_POSITION`,
       `constraint_name`         AS `CONSTRAINT_NAME`
FROM system.columns
WHERE is_in_primary_key;