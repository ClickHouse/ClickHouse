-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/67601
-- A metadata-only `ALTER` on a temporary view (e.g. `MODIFY COMMENT`) must not
-- re-register the session-local temporary view in the global plain-view dependency
-- graph exposed by `system.tables.dependencies_database` and
-- `system.tables.dependencies_table`. This complements the `CREATE`-path guard.

DROP TABLE IF EXISTS temporary_view_alter_source;

CREATE TABLE temporary_view_alter_source (id UInt64) ENGINE = Memory;

CREATE TEMPORARY VIEW temporary_view_over_source AS SELECT * FROM temporary_view_alter_source;

-- A metadata-only alter on the temporary view.
ALTER TABLE temporary_view_over_source MODIFY COMMENT 'a comment';

-- The temporary view must still not be reported as a dependent of the permanent
-- source table, and no hidden `_temporary_and_external_tables` / `_tmp_...` entry
-- must appear after the `ALTER`.
SELECT
    length(dependencies_table),
    has(dependencies_database, '_temporary_and_external_tables'),
    arrayExists(x -> startsWith(x, '_tmp_'), dependencies_table)
FROM system.tables
WHERE database = currentDatabase() AND name = 'temporary_view_alter_source';

DROP TABLE temporary_view_alter_source;
