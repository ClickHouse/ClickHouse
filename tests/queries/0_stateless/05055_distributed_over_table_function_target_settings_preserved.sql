-- Tags: no-fasttest
-- no-fasttest: the `mysql` and `PostgreSQL` table functions are not available in the fast test build.

-- The create-time analysis of a table-function target resolves its arguments in place, because the stored
-- definition has to be self-contained. It must not consume a function-local `SETTINGS` clause, though:
-- `parseArguments` of `mysql`, `PostgreSQL` and `ytsaurus` erases such a clause from the argument list, and
-- for these engines that list is the persisted definition. The addresses below are unreachable on purpose -
-- the target is analyzed but never connects, and the explicit column list keeps the `CREATE` alive.

DROP TABLE IF EXISTS distributed_mysql_target;
DROP TABLE IF EXISTS distributed_postgresql_target;
DROP TABLE IF EXISTS remote_mysql_target;

CREATE TABLE distributed_mysql_target (x Int32) ENGINE = Distributed(test_shard_localhost,
    mysql('127.123.0.1:3306', 'db', 't', 'u', 'p', SETTINGS connect_timeout = 12, read_write_timeout = 34));

CREATE TABLE distributed_postgresql_target (x Int32) ENGINE = Distributed(test_shard_localhost,
    PostgreSQL('127.121.0.1:5432', 'db', 't', 'u', 'p', SETTINGS connection_pool_size = 3));

CREATE TABLE remote_mysql_target (x Int32) ENGINE = Remote('127.0.0.1',
    mysql('127.123.0.1:3306', 'db', 't', 'u', 'p', SETTINGS connect_timeout = 12, read_write_timeout = 34));

SELECT name, engine_full LIKE '%SETTINGS connect_timeout = 12, read_write_timeout = 34%' AS mysql_settings_kept
FROM system.tables WHERE database = currentDatabase() AND name IN ('distributed_mysql_target', 'remote_mysql_target')
ORDER BY name;

SELECT engine_full LIKE '%SETTINGS connection_pool_size = 3%' AS postgresql_settings_kept
FROM system.tables WHERE database = currentDatabase() AND name = 'distributed_postgresql_target';

-- The definition survives a round trip through the metadata as well.
DETACH TABLE distributed_mysql_target;
ATTACH TABLE distributed_mysql_target;

SELECT engine_full LIKE '%SETTINGS connect_timeout = 12, read_write_timeout = 34%' AS mysql_settings_kept_after_reattach
FROM system.tables WHERE database = currentDatabase() AND name = 'distributed_mysql_target';

DROP TABLE distributed_mysql_target;
DROP TABLE distributed_postgresql_target;
DROP TABLE remote_mysql_target;
