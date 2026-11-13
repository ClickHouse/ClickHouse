-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/67601
-- A `CREATE TEMPORARY VIEW` is session-local and must not leak into the global
-- plain-view dependency graph exposed by `system.tables.dependencies_database`
-- and `system.tables.dependencies_table`.

DROP TABLE IF EXISTS temporary_view_source;

CREATE TABLE temporary_view_source (id UInt64) ENGINE = Memory;

-- No view yet: the source table has no view dependents.
SELECT length(dependencies_table)
FROM system.tables
WHERE database = currentDatabase() AND name = 'temporary_view_source';

CREATE TEMPORARY VIEW temporary_view_over_source AS SELECT * FROM temporary_view_source;

-- The temporary view must not be reported as a dependent of the permanent source table,
-- and no hidden `_temporary_and_external_tables` / `_tmp_...` entry must appear.
SELECT
    length(dependencies_table),
    has(dependencies_database, '_temporary_and_external_tables'),
    arrayExists(x -> startsWith(x, '_tmp_'), dependencies_table)
FROM system.tables
WHERE database = currentDatabase() AND name = 'temporary_view_source';

-- A permanent view is still reported (the fix is limited to temporary views).
CREATE VIEW permanent_view_over_source AS SELECT * FROM temporary_view_source;

SELECT
    dependencies_database = [currentDatabase()],
    dependencies_table
FROM system.tables
WHERE database = currentDatabase() AND name = 'temporary_view_source';

DROP VIEW permanent_view_over_source;
DROP TABLE temporary_view_source;
