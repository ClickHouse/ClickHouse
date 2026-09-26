-- Tags: no-parallel
-- Tag no-parallel: creates a `Remote` database pointing at a closed port. `show_remote_databases_in_system_tables`
-- defaults to true, so a concurrent `system.tables` or `system.columns` scan without a database filter would
-- try to reach it and fail.

-- `system.table_settings` on a `Remote` database. Its tables are `Distributed` storages, and it keeps the plain
-- table iterator, which is best-effort for `Remote`: an unreachable server yields no rows rather than an error,
-- also for a query that names the table. A data lake catalog takes the hinted iterator instead, which
-- would turn that into an error - see `test_database_iceberg::test_table_settings_for_datalake_catalog`.

SET send_logs_level = 'fatal';

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_2:Identifier};

-- Created while the test's own database is current: `Remote` evaluates `currentDatabase()` here.
DROP TABLE IF EXISTS t;
CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY a;
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Remote('127.0.0.1', currentDatabase());
CREATE DATABASE {CLICKHOUSE_DATABASE_2:Identifier} ENGINE = Remote('127.0.0.1:1', currentDatabase());

USE {CLICKHOUSE_DATABASE_1:Identifier};

SELECT '-- a table of a Remote database reports the settings of its Distributed storage';
SELECT name, value, source FROM system.table_settings
WHERE database = currentDatabase() AND table = 't' AND name = 'fsync_after_insert';

SELECT '-- hidden from system.table_settings with show_remote_databases_in_system_tables off';
SELECT count() FROM system.table_settings
WHERE database = currentDatabase() AND table = 't'
SETTINGS show_remote_databases_in_system_tables = 0;

SELECT '-- SHOW TABLE SETTINGS names the database, so it shows the table even then';
SET show_remote_databases_in_system_tables = 0;
SHOW TABLE SETTINGS FROM t LIKE 'fsync_after_insert';
SET show_remote_databases_in_system_tables = 1;

USE {CLICKHOUSE_DATABASE_2:Identifier};

SELECT '-- an unreachable Remote database: no rows, no error, whether listed by database or by table';
SELECT count() FROM system.table_settings WHERE database = currentDatabase();
SELECT count() FROM system.table_settings WHERE database = currentDatabase() AND table = 't';

SELECT '-- SHOW TABLE SETTINGS looks the table up first, and reports that the server is unreachable';
SHOW TABLE SETTINGS FROM t; -- { serverError NO_REMOTE_SHARD_AVAILABLE }
-- Also with the visibility setting off: the statement enables it for the named database, and the server is still unreachable.
SET show_remote_databases_in_system_tables = 0;
SHOW TABLE SETTINGS FROM t; -- { serverError NO_REMOTE_SHARD_AVAILABLE }
SET show_remote_databases_in_system_tables = 1;

USE {CLICKHOUSE_DATABASE:Identifier};
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
DROP DATABASE {CLICKHOUSE_DATABASE_2:Identifier};
DROP TABLE t;
