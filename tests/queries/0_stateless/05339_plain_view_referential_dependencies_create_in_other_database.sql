-- Regression test: `CREATE VIEW other_db.v` issued from a session whose current database is not the database of the
-- view must register the referential dependencies against the database that owns the view, as the metadata loading
-- path and `ALTER` do. Otherwise `check_referential_table_dependencies` protects a same-named table in the current
-- database of the session and allows dropping the actual source of the view right after `CREATE`.

SET check_referential_table_dependencies = 1;

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
DROP TABLE IF EXISTS create_other_db_ref_source;

-- A same-named table in the current database of the session.
CREATE TABLE create_other_db_ref_source (id UInt64) ENGINE = MergeTree ORDER BY id;

CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.create_other_db_ref_source (id UInt64) ENGINE = MergeTree ORDER BY id;

-- The temporary table shadowing the source is what keeps the stored definition of the view unqualified:
-- `AddDefaultDatabaseVisitor` does not qualify names of session-local external tables.
CREATE TEMPORARY TABLE create_other_db_ref_source (id UInt64) ENGINE = Memory;
CREATE VIEW {CLICKHOUSE_DATABASE_1:Identifier}.create_other_db_ref_view AS SELECT * FROM create_other_db_ref_source;

-- Guard the precondition: the source of the view is stored unqualified.
SELECT 'source is stored unqualified', position(create_table_query, '.create_other_db_ref_source') = 0
FROM system.tables WHERE database = {CLICKHOUSE_DATABASE_1:String} AND name = 'create_other_db_ref_view';

DROP TABLE {CLICKHOUSE_DATABASE_1:Identifier}.create_other_db_ref_source; -- { serverError HAVE_DEPENDENT_OBJECTS }
DROP TABLE {CLICKHOUSE_DATABASE:Identifier}.create_other_db_ref_source;

SELECT 'tables left', database = currentDatabase() AS is_session_db
FROM system.tables WHERE name = 'create_other_db_ref_source' AND database IN (currentDatabase(), {CLICKHOUSE_DATABASE_1:String});

DROP TEMPORARY TABLE create_other_db_ref_source;
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
