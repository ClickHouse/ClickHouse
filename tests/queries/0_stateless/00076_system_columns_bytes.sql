-- Tags: stateful
-- NOTE:
-- - database = currentDatabase() is not mandatory
-- - Merge tables may cause UNKNOWN_DATABASE/CANNOT_EXTRACT_TABLE_STRUCTURE from StorageMerge::getColumnSizes() since the table/database can be removed
-- - StorageProxy can wrap any table function, so they also have to be excluded if their nested table function is merge
SELECT
    sum(data_compressed_bytes) > 0,
    sum(data_uncompressed_bytes) > 0,
    sum(marks_bytes) > 0
FROM system.columns
WHERE (database, `table`) IN (
    SELECT
        database,
        `table`
    FROM system.tables
    WHERE engine != 'Merge' AND (engine != 'StorageProxy' OR create_table_query NOT ILIKE '%merge%')
)