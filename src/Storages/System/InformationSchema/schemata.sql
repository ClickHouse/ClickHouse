ATTACH VIEW schemata
(
    `catalog_name` String,
    `schema_name` String,
    `schema_owner` String,
    `default_character_set_catalog` Nullable(String),
    `default_character_set_schema` Nullable(String),
    `default_character_set_name` Nullable(String),
    `sql_path` Nullable(String),
    `CATALOG_NAME` String,
    `SCHEMA_NAME` String,
    `SCHEMA_OWNER` String,
    `DEFAULT_CHARACTER_SET_CATALOG` Nullable(String),
    `DEFAULT_CHARACTER_SET_SCHEMA` Nullable(String),
    `DEFAULT_CHARACTER_SET_NAME` Nullable(String),
    `SQL_PATH` Nullable(String)
) AS
SELECT
    name                          AS catalog_name,
    name                          AS schema_name,
    'default'                     AS schema_owner,
    NULL                          AS default_character_set_catalog,
    NULL                          AS default_character_set_schema,
    NULL                          AS default_character_set_name,
    NULL                          AS sql_path,
    catalog_name                  AS CATALOG_NAME,
    schema_name                   AS SCHEMA_NAME,
    schema_owner                  AS SCHEMA_OWNER,
    default_character_set_catalog AS DEFAULT_CHARACTER_SET_CATALOG,
    default_character_set_schema  AS DEFAULT_CHARACTER_SET_SCHEMA,
    default_character_set_name    AS DEFAULT_CHARACTER_SET_NAME,
    sql_path                      AS SQL_PATH
FROM system.databases
