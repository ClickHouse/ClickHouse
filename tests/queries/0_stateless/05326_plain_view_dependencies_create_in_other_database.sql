-- Regression test: `CREATE VIEW other_db.v` issued from a session whose current database is not the database of the
-- view must register the plain-view dependencies against the database that owns the view, as the metadata loading
-- path and `ALTER` do. Otherwise `system.tables.dependencies_*` is wrong right after `CREATE` and changes later,
-- on a metadata-only `ALTER`, without the query of the view changing.

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
DROP TABLE IF EXISTS create_other_db_source;

-- A same-named table in the current database of the session.
CREATE TABLE create_other_db_source (id UInt64) ENGINE = MergeTree ORDER BY id;

CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.create_other_db_source (id UInt64) ENGINE = MergeTree ORDER BY id;

-- The temporary table shadowing the source is what keeps the stored definition of the view unqualified:
-- `AddDefaultDatabaseVisitor` does not qualify names of session-local external tables.
CREATE TEMPORARY TABLE create_other_db_source (id UInt64) ENGINE = Memory;
CREATE VIEW {CLICKHOUSE_DATABASE_1:Identifier}.create_other_db_view AS SELECT * FROM create_other_db_source;

-- Guard the precondition: the source of the view is stored unqualified.
SELECT 'source is stored unqualified', position(create_table_query, '.create_other_db_source') = 0
FROM system.tables WHERE database = {CLICKHOUSE_DATABASE_1:String} AND name = 'create_other_db_view';

SELECT 'after create', if(database = currentDatabase(), 'session_db', 'views_db') AS db, dependencies_table
FROM system.tables WHERE name = 'create_other_db_source' AND database IN (currentDatabase(), {CLICKHOUSE_DATABASE_1:String}) ORDER BY db;

ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.create_other_db_view MODIFY COMMENT 'unchanged query';

SELECT 'after alter', if(database = currentDatabase(), 'session_db', 'views_db') AS db, dependencies_table
FROM system.tables WHERE name = 'create_other_db_source' AND database IN (currentDatabase(), {CLICKHOUSE_DATABASE_1:String}) ORDER BY db;

DROP TEMPORARY TABLE create_other_db_source;
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
DROP TABLE create_other_db_source;
