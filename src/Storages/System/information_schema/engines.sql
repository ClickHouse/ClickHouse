ATTACH VIEW engines
(
    engine String,
    support String,
    ENGINE String,
    SUPPORT String
)
SQL SECURITY INVOKER
AS SELECT
    name AS engine,
    engine = getSetting('default_table_engine') ? 'DEFAULT' : 'YES' AS support,
    engine AS ENGINE,
    support AS SUPPORT
FROM system.table_engines
