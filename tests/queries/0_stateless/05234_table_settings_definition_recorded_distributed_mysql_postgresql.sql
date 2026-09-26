-- Tags: no-fasttest
-- Tag justification: depends on the `MySQL` and `PostgreSQL` table engines, which are not built in fast test.
--
-- `Distributed`, `MySQL` and `PostgreSQL` record the table's own `SETTINGS` clause in the settings object as
-- they apply it, so `system.table_settings` reads the source from there rather than from the stored `CREATE`
-- query. The two have to agree on `CREATE` and after the table is loaded again from what it stored; none of the
-- three supports a settings `ALTER`. A setting stated under an alias is recorded under its own name.
--
-- Neither `MySQL` nor `PostgreSQL` connects at `CREATE` when the columns are given, so an unreachable host is
-- fine: nothing is ever connected.

DROP TABLE IF EXISTS dist_definition;
DROP TABLE IF EXISTS dist_definition_src;
DROP TABLE IF EXISTS mysql_definition;
DROP TABLE IF EXISTS postgresql_definition;

CREATE TABLE dist_definition_src (a UInt64) ENGINE = Memory;
CREATE TABLE dist_definition AS dist_definition_src
ENGINE = Distributed('test_shard_localhost', currentDatabase(), 'dist_definition_src')
SETTINGS bytes_to_delay_insert = 123456, monitor_batch_inserts = 1;

CREATE TABLE mysql_definition (x Int32) ENGINE = MySQL('unreachable.invalid:3306', 'db', 'tbl', 'user', 'password')
SETTINGS connection_pool_size = 7;

CREATE TABLE postgresql_definition (x Int32)
ENGINE = PostgreSQL('unreachable.invalid:5432', 'db', 'tbl', 'user', 'password')
SETTINGS postgresql_connection_pool_retries = 5;

SELECT '-- CREATE';
SELECT table, name, value, source FROM system.table_settings
WHERE database = currentDatabase()
    AND ((table = 'dist_definition' AND name IN ('bytes_to_delay_insert', 'background_insert_batch', 'max_delay_to_insert'))
        OR (table = 'mysql_definition' AND name IN ('connection_pool_size', 'connection_max_tries'))
        OR (table = 'postgresql_definition' AND name IN ('postgresql_connection_pool_retries', 'postgresql_connection_attempt_timeout')))
ORDER BY table, name;

DETACH TABLE dist_definition;
ATTACH TABLE dist_definition;
DETACH TABLE mysql_definition;
ATTACH TABLE mysql_definition;
DETACH TABLE postgresql_definition;
ATTACH TABLE postgresql_definition;

SELECT '-- loaded again from what they stored';
SELECT table, name, value, source FROM system.table_settings
WHERE database = currentDatabase()
    AND ((table = 'dist_definition' AND name IN ('bytes_to_delay_insert', 'background_insert_batch', 'max_delay_to_insert'))
        OR (table = 'mysql_definition' AND name IN ('connection_pool_size', 'connection_max_tries'))
        OR (table = 'postgresql_definition' AND name IN ('postgresql_connection_pool_retries', 'postgresql_connection_attempt_timeout')))
ORDER BY table, name;

DROP TABLE dist_definition;
DROP TABLE dist_definition_src;
DROP TABLE mysql_definition;
DROP TABLE postgresql_definition;
