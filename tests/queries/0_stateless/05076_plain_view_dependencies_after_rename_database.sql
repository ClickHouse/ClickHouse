-- Regression test: `RENAME DATABASE` must re-key a plain view under its new database name in the
-- plain-view dependency graph, so that the source table reports the view under the new name in
-- `system.tables.dependencies_*`, and a later `ALTER` / `DROP` of the renamed view finds and cleans its edge.
-- The pre-existing `ReferentialDeps` / `ViewDeps` graphs may log warnings on such operations; they are not what this test checks.
SET send_logs_level = 'error';

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_2:Identifier};
DROP TABLE IF EXISTS rename_db_source;

CREATE TABLE rename_db_source (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO rename_db_source VALUES (42);

CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic;
-- The source is written unqualified on purpose: a query parameter inside the view body would make this a parameterized view.
-- It is qualified with the current database (the one of the source) when the view is created.
CREATE VIEW {CLICKHOUSE_DATABASE_1:Identifier}.rename_db_view AS SELECT * FROM rename_db_source;

-- Guard the precondition: the source of the view is stored qualified with the database of the source.
SELECT 'source is stored qualified', position(create_table_query, concat(currentDatabase(), '.rename_db_source')) > 0
FROM system.tables WHERE database = {CLICKHOUSE_DATABASE_1:String} AND name = 'rename_db_view';

-- The random database names are replaced by stable labels, so the reference file is deterministic.
SELECT 'before rename', arraySort(arrayMap((d, t) -> concat(multiIf(d = {CLICKHOUSE_DATABASE_1:String}, 'db1', d = {CLICKHOUSE_DATABASE_2:String}, 'db2', d), '.', t), dependencies_database, dependencies_table))
FROM system.tables WHERE database = currentDatabase() AND name = 'rename_db_source';

RENAME DATABASE {CLICKHOUSE_DATABASE_1:Identifier} TO {CLICKHOUSE_DATABASE_2:Identifier};

SELECT 'after rename', arraySort(arrayMap((d, t) -> concat(multiIf(d = {CLICKHOUSE_DATABASE_1:String}, 'db1', d = {CLICKHOUSE_DATABASE_2:String}, 'db2', d), '.', t), dependencies_database, dependencies_table))
FROM system.tables WHERE database = currentDatabase() AND name = 'rename_db_source';

-- The view keeps working under the new name (its stored SELECT refers to the source by a qualified name).
SELECT 'view reads', * FROM {CLICKHOUSE_DATABASE_2:Identifier}.rename_db_view;

-- A metadata-only ALTER under the new name must not leave a stale edge under the old name behind.
ALTER TABLE {CLICKHOUSE_DATABASE_2:Identifier}.rename_db_view MODIFY COMMENT 'renamed';

SELECT 'after alter', arraySort(arrayMap((d, t) -> concat(multiIf(d = {CLICKHOUSE_DATABASE_1:String}, 'db1', d = {CLICKHOUSE_DATABASE_2:String}, 'db2', d), '.', t), dependencies_database, dependencies_table))
FROM system.tables WHERE database = currentDatabase() AND name = 'rename_db_source';

DROP VIEW {CLICKHOUSE_DATABASE_2:Identifier}.rename_db_view;

SELECT 'after drop', arraySort(arrayMap((d, t) -> concat(multiIf(d = {CLICKHOUSE_DATABASE_1:String}, 'db1', d = {CLICKHOUSE_DATABASE_2:String}, 'db2', d), '.', t), dependencies_database, dependencies_table))
FROM system.tables WHERE database = currentDatabase() AND name = 'rename_db_source';

DROP DATABASE {CLICKHOUSE_DATABASE_2:Identifier};
DROP TABLE rename_db_source;
