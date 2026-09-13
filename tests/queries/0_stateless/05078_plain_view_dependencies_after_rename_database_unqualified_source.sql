-- Regression test: when the stored `SELECT` of a plain view names its source without a database, that
-- source is resolved against the database owning the view, so `RENAME DATABASE` moves both ends of the
-- plain-view dependency edge into the new database and the source keeps reporting the view in
-- `system.tables.dependencies_*`.
-- The pre-existing `ReferentialDeps` / `ViewDeps` graphs may log warnings on such operations; they are not what this test checks.
SET send_logs_level = 'error';

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_2:Identifier};
DROP TABLE IF EXISTS rename_db_unqualified_src;

-- A same-named table in the current database: the target the dependency must never move to.
CREATE TABLE rename_db_unqualified_src (id UInt64) ENGINE = MergeTree ORDER BY id;

CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic;
USE {CLICKHOUSE_DATABASE_1:Identifier};

CREATE TABLE rename_db_unqualified_src (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO rename_db_unqualified_src VALUES (42);

-- The temporary table shadowing the source is what keeps the stored definition of the view unqualified:
-- `AddDefaultDatabaseVisitor` does not qualify names of session-local external tables.
-- Do not remove it: without it the stored definition is already qualified and the rename below checks nothing.
CREATE TEMPORARY TABLE rename_db_unqualified_src (id UInt64) ENGINE = Memory;
CREATE VIEW rename_db_unqualified_view AS SELECT * FROM rename_db_unqualified_src;

USE {CLICKHOUSE_DATABASE:Identifier};

-- Guard the precondition: the source of the view is stored without a database name.
SELECT 'source is stored unqualified', position(create_table_query, '.rename_db_unqualified_src') = 0
FROM system.tables WHERE database = {CLICKHOUSE_DATABASE_1:String} AND name = 'rename_db_unqualified_view';

-- The random database names are replaced by stable labels, so the reference file is deterministic.
SELECT 'before rename', arraySort(arrayMap((d, t) -> concat(multiIf(d = {CLICKHOUSE_DATABASE_1:String}, 'db1', d = {CLICKHOUSE_DATABASE_2:String}, 'db2', d), '.', t), dependencies_database, dependencies_table))
FROM system.tables WHERE database = {CLICKHOUSE_DATABASE_1:String} AND name = 'rename_db_unqualified_src';

RENAME DATABASE {CLICKHOUSE_DATABASE_1:Identifier} TO {CLICKHOUSE_DATABASE_2:Identifier};

SELECT 'after rename', arraySort(arrayMap((d, t) -> concat(multiIf(d = {CLICKHOUSE_DATABASE_1:String}, 'db1', d = {CLICKHOUSE_DATABASE_2:String}, 'db2', d), '.', t), dependencies_database, dependencies_table))
FROM system.tables WHERE database = {CLICKHOUSE_DATABASE_2:String} AND name = 'rename_db_unqualified_src';

-- The edge follows the view into the new database, it does not land on the same-named table elsewhere.
SELECT 'same-named table elsewhere', dependencies_table
FROM system.tables WHERE database = currentDatabase() AND name = 'rename_db_unqualified_src';

-- The referential dependency is resolved the same way, so the source in the new database stays
-- protected from `DROP` while the renamed view reads it.
SET check_referential_table_dependencies = 1;
SET send_logs_level = 'fatal'; -- the expected error below must not be sent to the client
DROP TABLE {CLICKHOUSE_DATABASE_2:Identifier}.rename_db_unqualified_src; -- { serverError HAVE_DEPENDENT_OBJECTS }
SET send_logs_level = 'error';
SET check_referential_table_dependencies = 0;

-- An unqualified source is resolved against the current database, so read the view from its own database:
-- the row it returns comes from the table the re-keyed edge points at.
DROP TEMPORARY TABLE rename_db_unqualified_src;
USE {CLICKHOUSE_DATABASE_2:Identifier};
SELECT 'view reads', * FROM rename_db_unqualified_view;
USE {CLICKHOUSE_DATABASE:Identifier};

-- A metadata-only ALTER recomputes the edge from the stored definition and must land on the same source.
ALTER TABLE {CLICKHOUSE_DATABASE_2:Identifier}.rename_db_unqualified_view MODIFY COMMENT 'renamed';

SELECT 'after alter', arraySort(arrayMap((d, t) -> concat(multiIf(d = {CLICKHOUSE_DATABASE_1:String}, 'db1', d = {CLICKHOUSE_DATABASE_2:String}, 'db2', d), '.', t), dependencies_database, dependencies_table))
FROM system.tables WHERE database = {CLICKHOUSE_DATABASE_2:String} AND name = 'rename_db_unqualified_src';

DROP VIEW {CLICKHOUSE_DATABASE_2:Identifier}.rename_db_unqualified_view;

SELECT 'after drop', arraySort(arrayMap((d, t) -> concat(multiIf(d = {CLICKHOUSE_DATABASE_1:String}, 'db1', d = {CLICKHOUSE_DATABASE_2:String}, 'db2', d), '.', t), dependencies_database, dependencies_table))
FROM system.tables WHERE database = {CLICKHOUSE_DATABASE_2:String} AND name = 'rename_db_unqualified_src';

DROP DATABASE {CLICKHOUSE_DATABASE_2:Identifier};
DROP TABLE rename_db_unqualified_src;
