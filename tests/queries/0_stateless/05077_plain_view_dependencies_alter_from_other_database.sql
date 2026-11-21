-- Regression test: a metadata-only `ALTER` of a plain view (e.g. `MODIFY COMMENT`) must not move the
-- dependencies of the view when it is issued from a session whose current database is not the database of the view.
-- The stored definition is re-parsed on `ALTER`, and unqualified source names in it must be resolved against
-- the database that owns the view, as on the metadata loading path, not against the current database of the session.

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
DROP TABLE IF EXISTS alter_other_db_source;

-- A same-named table in the current database: the target the dependency used to move to.
CREATE TABLE alter_other_db_source (id UInt64) ENGINE = MergeTree ORDER BY id;

CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
USE {CLICKHOUSE_DATABASE_1:Identifier};
CREATE TABLE alter_other_db_source (id UInt64) ENGINE = MergeTree ORDER BY id;

-- The temporary table shadowing the source is what keeps the stored definition of the view unqualified:
-- `AddDefaultDatabaseVisitor` does not qualify names of session-local external tables.
-- Do not remove it: without it the stored definition is already qualified and the `ALTER` below checks nothing.
CREATE TEMPORARY TABLE alter_other_db_source (id UInt64) ENGINE = Memory;
CREATE VIEW alter_other_db_view AS SELECT * FROM alter_other_db_source;

USE {CLICKHOUSE_DATABASE:Identifier};

-- Guard the precondition: the source of the view is stored unqualified.
SELECT 'source is stored unqualified', position(create_table_query, '.alter_other_db_source') = 0
FROM system.tables WHERE database = {CLICKHOUSE_DATABASE_1:String} AND name = 'alter_other_db_view';

SELECT 'before alter', if(database = currentDatabase(), 'other_db', 'views_db') AS db, dependencies_table
FROM system.tables WHERE name = 'alter_other_db_source' AND database IN (currentDatabase(), {CLICKHOUSE_DATABASE_1:String}) ORDER BY db;

-- Issued with the current database being the other one.
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.alter_other_db_view MODIFY COMMENT 'unchanged query';

SELECT 'after alter', if(database = currentDatabase(), 'other_db', 'views_db') AS db, dependencies_table
FROM system.tables WHERE name = 'alter_other_db_source' AND database IN (currentDatabase(), {CLICKHOUSE_DATABASE_1:String}) ORDER BY db;

-- The referential dependency is recomputed on the same path and must stay with the source in the view's database.
SET check_referential_table_dependencies = 1;
DROP TABLE {CLICKHOUSE_DATABASE_1:Identifier}.alter_other_db_source; -- { serverError HAVE_DEPENDENT_OBJECTS }
DROP TABLE {CLICKHOUSE_DATABASE:Identifier}.alter_other_db_source;
SET check_referential_table_dependencies = 0;

DROP TEMPORARY TABLE alter_other_db_source;
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
