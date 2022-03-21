ATTACH VIEW schemata
(
    `catalog_name` String,
    `schema_name` String,
    `schema_owner` String,
    `default_character_set_catalog` Nullable(String),
    `default_character_set_schema` Nullable(String),
    `default_character_set_name` Nullable(String),
    `sql_path` Nullable(String),
    `CATALOG_NAME` String ALIAS catalog_name,
    `SCHEMA_NAME` String ALIAS schema_name,
    `SCHEMA_OWNER` String ALIAS schema_owner,
    `DEFAULT_CHARACTER_SET_CATALOG` Nullable(String) ALIAS default_character_set_catalog,
    `DEFAULT_CHARACTER_SET_SCHEMA` Nullable(String) ALIAS default_character_set_schema,
    `DEFAULT_CHARACTER_SET_NAME` Nullable(String) ALIAS default_character_set_name,
    `SQL_PATH` Nullable(String) ALIAS sql_path
) AS
SELECT
    name AS catalog_name,
    name AS schema_name,
    'default' AS schema_owner,
    NULL AS default_character_set_catalog,
    NULL AS default_character_set_schema,
    NULL AS default_character_set_name,
    NULL AS sql_path
FROM system.databases
