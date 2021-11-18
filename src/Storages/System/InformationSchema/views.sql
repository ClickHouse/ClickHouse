ATTACH VIEW views
(
    `table_catalog` String,
    `table_schema` String,
    `table_name` String,
    `view_definition` String,
    `check_option` String,
    `is_updatable` Enum8('NO' = 0, 'YES' = 1),
    `is_insertable_into` Enum8('NO' = 0, 'YES' = 1),
    `is_trigger_updatable` Enum8('NO' = 0, 'YES' = 1),
    `is_trigger_deletable` Enum8('NO' = 0, 'YES' = 1),
    `is_trigger_insertable_into` Enum8('NO' = 0, 'YES' = 1),
    `TABLE_CATALOG` String ALIAS table_catalog,
    `TABLE_SCHEMA` String ALIAS table_schema,
    `TABLE_NAME` String ALIAS table_name,
    `VIEW_DEFINITION` String ALIAS view_definition,
    `CHECK_OPTION` String ALIAS check_option,
    `IS_UPDATABLE` Enum8('NO' = 0, 'YES' = 1) ALIAS is_updatable,
    `IS_INSERTABLE_INTO` Enum8('NO' = 0, 'YES' = 1) ALIAS is_insertable_into,
    `IS_TRIGGER_UPDATABLE` Enum8('NO' = 0, 'YES' = 1) ALIAS is_trigger_updatable,
    `IS_TRIGGER_DELETABLE` Enum8('NO' = 0, 'YES' = 1) ALIAS is_trigger_deletable,
    `IS_TRIGGER_INSERTABLE_INTO` Enum8('NO' = 0, 'YES' = 1) ALIAS is_trigger_insertable_into
) AS
SELECT
    database AS table_catalog,
    database AS table_schema,
    name AS table_name,
    as_select AS view_definition,
    'NONE' AS check_option,
    0 AS is_updatable,
    engine = 'MaterializedView' AS is_insertable_into,
    0 AS is_trigger_updatable,
    0 AS is_trigger_deletable,
    0 AS is_trigger_insertable_into
FROM system.tables
WHERE engine LIKE '%View'
