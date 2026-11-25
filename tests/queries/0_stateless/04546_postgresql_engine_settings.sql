-- Tags: no-fasttest, no-parallel, no-replicated-database
-- Tag justification:
--   no-fasttest: depends on libpqxx (the PostgreSQL table engine), which is not built in fast test.
--   no-parallel: creates and drops global named collections; the flaky check runs the same test
--                concurrently, and parallel repetitions collide on them (the first finishing run
--                drops the collection while others still use it).
--   no-replicated-database: named collections are server-global, not database-scoped.
--
-- The PostgreSQL table engine does not connect at CREATE time when the columns are given explicitly and
-- the connection pool is created lazily, so an unreachable host is fine here: nothing is ever connected.

SET send_logs_level = 'fatal';

DROP TABLE IF EXISTS t_pg_settings;

-- The engine used to reject any SETTINGS clause with "Engine PostgreSQL doesn't support SETTINGS clause".
CREATE TABLE t_pg_settings (x Int32)
ENGINE = PostgreSQL('127.0.0.1:5432', 'db', 'tbl', 'user', 'password')
SETTINGS postgresql_connection_pool_size = 50;

SELECT '--- single setting is accepted and shown in SHOW CREATE ---';
SHOW CREATE TABLE t_pg_settings;
DROP TABLE t_pg_settings;

CREATE TABLE t_pg_settings (x Int32)
ENGINE = PostgreSQL('127.0.0.1:5432', 'db', 'tbl', 'user', 'password')
SETTINGS
    postgresql_connection_pool_size = 8,
    postgresql_connection_pool_wait_timeout = 1000,
    postgresql_connection_pool_retries = 4,
    postgresql_connection_pool_auto_close_connection = 1,
    postgresql_connection_attempt_timeout = 3;

SELECT '--- all pool settings are accepted ---';
SHOW CREATE TABLE t_pg_settings;
DROP TABLE t_pg_settings;

SELECT '--- unknown engine setting is rejected ---';
CREATE TABLE t_pg_settings (x Int32)
ENGINE = PostgreSQL('127.0.0.1:5432', 'db', 'tbl', 'user', 'password')
SETTINGS not_a_real_setting = 1; -- { serverError UNKNOWN_SETTING }

SELECT '--- zero pool size is rejected ---';
CREATE TABLE t_pg_settings (x Int32)
ENGINE = PostgreSQL('127.0.0.1:5432', 'db', 'tbl', 'user', 'password')
SETTINGS postgresql_connection_pool_size = 0; -- { serverError BAD_ARGUMENTS }

-- Query-level settings mixed into the same SETTINGS clause are separated from the engine settings and
-- must not appear in the stored table definition.
CREATE TABLE t_pg_settings (x Int32)
ENGINE = PostgreSQL('127.0.0.1:5432', 'db', 'tbl', 'user', 'password')
SETTINGS postgresql_connection_pool_size = 3, max_threads = 1;

SELECT '--- engine settings and query settings can be mixed ---';
SHOW CREATE TABLE t_pg_settings;
DROP TABLE t_pg_settings;

SELECT '--- the postgresql table function validates its settings too ---';
SELECT * FROM postgresql('127.0.0.1:5432', 'db', 'tbl', 'user', 'password', SETTINGS postgresql_connection_pool_size = 0); -- { serverError BAD_ARGUMENTS }
SELECT * FROM postgresql('127.0.0.1:5432', 'db', 'tbl', 'user', 'password', SETTINGS not_a_real_setting = 1); -- { serverError UNKNOWN_SETTING }

-- The pool settings can also be passed as key=value overrides on a named collection, like the `MySQL` engine.
CREATE NAMED COLLECTION IF NOT EXISTS pg_settings_nc AS host = '127.0.0.1', port = 5432, database = 'db', table = 'tbl', user = 'user', password = 'password';

CREATE TABLE t_pg_settings (x Int32)
ENGINE = PostgreSQL(pg_settings_nc, postgresql_connection_pool_size = 50);

SELECT '--- named collection option is accepted ---';
SHOW CREATE TABLE t_pg_settings;
DROP TABLE t_pg_settings;

SELECT '--- unknown named collection option is rejected ---';
CREATE TABLE t_pg_settings (x Int32)
ENGINE = PostgreSQL(pg_settings_nc, not_a_real_setting = 1); -- { serverError BAD_ARGUMENTS }

SELECT '--- zero pool size is rejected when passed through a named collection ---';
CREATE TABLE t_pg_settings (x Int32)
ENGINE = PostgreSQL(pg_settings_nc, postgresql_connection_pool_size = 0); -- { serverError BAD_ARGUMENTS }

-- An explicit SETTINGS clause has the highest precedence: it is applied after the values stored in the
-- named collection. `pg_settings_nc_pool` embeds a valid pool size, and the inline zero must still win
-- (and be rejected); if the named collection won, no error would be raised here.
CREATE NAMED COLLECTION IF NOT EXISTS pg_settings_nc_pool AS host = '127.0.0.1', port = 5432, database = 'db', table = 'tbl', user = 'user', password = 'password', postgresql_connection_pool_size = 8;

SELECT '--- the SETTINGS clause of the table function wins over the named collection ---';
SELECT * FROM postgresql(pg_settings_nc_pool, SETTINGS postgresql_connection_pool_size = 0); -- { serverError BAD_ARGUMENTS }

SELECT '--- the SETTINGS clause of the table engine wins over the named collection ---';
CREATE TABLE t_pg_settings (x Int32)
ENGINE = PostgreSQL(pg_settings_nc_pool)
SETTINGS postgresql_connection_pool_size = 0; -- { serverError BAD_ARGUMENTS }

-- The `PostgreSQL` database engine builds a connection pool as well, so it honors (and validates) the
-- pool settings from a named collection. The collection must not contain a `table` key for a database.
CREATE NAMED COLLECTION IF NOT EXISTS pg_settings_nc_db AS host = '127.0.0.1', port = 5432, database = 'db', user = 'user', password = 'password';

SELECT '--- the PostgreSQL database engine honors pool settings from a named collection ---';
CREATE DATABASE db_04546_pg_settings ENGINE = PostgreSQL(pg_settings_nc_db, postgresql_connection_pool_size = 50);
SHOW CREATE DATABASE db_04546_pg_settings;
DROP DATABASE db_04546_pg_settings;

SELECT '--- the PostgreSQL database engine rejects a zero pool size ---';
CREATE DATABASE db_04546_pg_settings ENGINE = PostgreSQL(pg_settings_nc_db, postgresql_connection_pool_size = 0); -- { serverError BAD_ARGUMENTS }

-- The MaterializedPostgreSQL engines do not use a connection pool, so the pool settings are rejected
-- instead of being accepted and silently ignored.
SELECT '--- the MaterializedPostgreSQL database engine rejects pool settings ---';
SET allow_experimental_database_materialized_postgresql = 1;
CREATE DATABASE db_04546_pg_settings ENGINE = MaterializedPostgreSQL(pg_settings_nc_db, postgresql_connection_pool_size = 50); -- { serverError BAD_ARGUMENTS }

SELECT '--- the MaterializedPostgreSQL table engine rejects pool settings ---';
SET allow_experimental_materialized_postgresql_table = 1;
CREATE TABLE t_pg_settings (x Int32)
ENGINE = MaterializedPostgreSQL(pg_settings_nc, postgresql_connection_pool_size = 50)
ORDER BY x; -- { serverError BAD_ARGUMENTS }

DROP NAMED COLLECTION pg_settings_nc_db;
DROP NAMED COLLECTION pg_settings_nc_pool;
DROP NAMED COLLECTION pg_settings_nc;
