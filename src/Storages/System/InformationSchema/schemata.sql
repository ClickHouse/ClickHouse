ATTACH VIEW schemata
(
    `catalog_name` String,
    `schema_name` String,
    `schema_owner` String,
    `default_character_set_catalog` Nullable(Nothing),
    `default_character_set_schema` Nullable(Nothing),
    `default_character_set_name` Nullable(Nothing),
    `sql_path` Nullable(Nothing),
    `CATALOG_NAME` String ALIAS catalog_name,
    `SCHEMA_NAME` String ALIAS schema_name,
    `SCHEMA_OWNER` String ALIAS schema_owner,
    `DEFAULT_CHARACTER_SET_CATALOG` Nullable(Nothing) ALIAS default_character_set_catalog,
    `DEFAULT_CHARACTER_SET_SCHEMA` Nullable(Nothing) ALIAS default_character_set_schema,
    `DEFAULT_CHARACTER_SET_NAME` Nullable(Nothing) ALIAS default_character_set_name
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
