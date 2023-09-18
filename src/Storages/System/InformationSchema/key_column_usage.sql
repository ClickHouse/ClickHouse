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
     `REFERENCED_TABLE_SCHEMA` Nullable(String) ALIAS referenced_table_schema,
     `REFERENCED_TABLE_NAME` Nullable(String) ALIAS referenced_table_name,
     `REFERENCED_COLUMN_NAME` Nullable(String) ALIAS referenced_column_name,
     `TABLE_SCHEMA` String ALIAS table_schema,
     `TABLE_NAME` String ALIAS table_name,
     `COLUMN_NAME` Nullable(String) ALIAS column_name,
     `ORDINAL_POSITION` UInt32 ALIAS ordinal_position,
     `CONSTRAINT_NAME` Nullable(String) ALIAS constraint_name
) AS
SELECT NULL      AS `referenced_table_schema`,
       NULL      AS `referenced_table_name`,
       NULL      AS `referenced_column_name`,
       database  AS `table_schema`,
       table     AS `table_name`,
       name      AS `column_name`,
       position  AS `ordinal_position`,
       'PRIMARY' AS `constraint_name`
FROM system.columns
WHERE is_in_primary_key;
