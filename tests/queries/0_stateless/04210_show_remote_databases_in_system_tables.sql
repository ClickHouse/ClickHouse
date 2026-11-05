-- Tags: no-fasttest, no-parallel
-- Tag justification:
--   no-fasttest: depends on libpq and libmysql (PostgreSQL and MySQL database engines),
--     which are not built in fast test.
--   no-parallel: creates PostgreSQL and MySQL databases pointing at an unreachable host.
--     Because `show_remote_databases_in_system_tables` defaults to `true`, these databases
--     are visible in `system.tables`, `system.columns` and `system.completions`, so any
--     concurrent query that scans those tables without a database filter would try to
--     connect to the unreachable host and fail with `POSTGRESQL_CONNECTION_FAILURE`.

SET send_logs_level = 'fatal';

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_2:Identifier};

-- PostgreSQL database engine does not connect at CREATE time, so a non-existent host is fine here.
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = PostgreSQL('192.0.2.1:5432', 'fake_db', 'user', 'password');

-- MySQL probes the server at CREATE time, but is tolerant of failures on ATTACH.
-- Use a short connect_timeout / single try so the tolerated failure happens fast.
ATTACH DATABASE {CLICKHOUSE_DATABASE_2:Identifier} ENGINE = MySQL('192.0.2.1:3306', 'fake_db', 'user', 'password') SETTINGS connect_timeout = 1, connection_max_tries = 1;

SELECT '--- system.databases always shows remote databases, regardless of the setting ---';
SELECT engine FROM system.databases WHERE name IN ({CLICKHOUSE_DATABASE_1:String}, {CLICKHOUSE_DATABASE_2:String}) ORDER BY engine SETTINGS show_remote_databases_in_system_tables = 0;
SELECT engine FROM system.databases WHERE name IN ({CLICKHOUSE_DATABASE_1:String}, {CLICKHOUSE_DATABASE_2:String}) ORDER BY engine SETTINGS show_remote_databases_in_system_tables = 1;

SELECT '--- system.tables hides PostgreSQL when disabled (count must be 0 without contacting the server) ---';
USE {CLICKHOUSE_DATABASE_1:Identifier};
SELECT count() FROM system.tables WHERE database = currentDatabase() SETTINGS show_remote_databases_in_system_tables = 0;

SELECT '--- system.tables hides MySQL when disabled ---';
USE {CLICKHOUSE_DATABASE_2:Identifier};
SELECT count() FROM system.tables WHERE database = currentDatabase() SETTINGS show_remote_databases_in_system_tables = 0;

SELECT '--- system.columns hides PostgreSQL when disabled ---';
USE {CLICKHOUSE_DATABASE_1:Identifier};
SELECT count() FROM system.columns WHERE database = currentDatabase() SETTINGS show_remote_databases_in_system_tables = 0;

SELECT '--- system.columns hides MySQL when disabled ---';
USE {CLICKHOUSE_DATABASE_2:Identifier};
SELECT count() FROM system.columns WHERE database = currentDatabase() SETTINGS show_remote_databases_in_system_tables = 0;

SELECT '--- system.completions does not list remote databases when disabled (count must be 0 without contacting the server) ---';
SELECT count() FROM system.completions WHERE word IN ({CLICKHOUSE_DATABASE_1:String}, {CLICKHOUSE_DATABASE_2:String}) AND context = 'database' SETTINGS show_remote_databases_in_system_tables = 0;

SELECT '--- system.settings exposes separate data-lake and remote-database settings ---';
SELECT name, value, alias_for = '' FROM system.settings WHERE name IN ('show_remote_databases_in_system_tables', 'show_data_lake_catalogs_in_system_tables') ORDER BY name;

USE {CLICKHOUSE_DATABASE:Identifier};
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
DROP DATABASE {CLICKHOUSE_DATABASE_2:Identifier};
