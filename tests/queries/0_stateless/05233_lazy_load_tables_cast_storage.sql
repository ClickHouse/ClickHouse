-- Tags: zookeeper, no-replicated-database
--       no-replicated-database: `DETACH DATABASE` / `ATTACH DATABASE` of an `Atomic` database
--       with the `lazy_load_tables` setting.

-- In a database with `lazy_load_tables = 1`, an unloaded table is a `StorageTableProxy`, and a cast of
-- the catalog pointer to the engine type sees only the proxy. The system tables and the replica
-- commands resolve the proxy through `castStorage` instead: a listing skips an unloaded table and
-- shows it once it is loaded, while a command that names the table loads it.
-- https://github.com/ClickHouse/ClickHouse/issues/117420

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic SETTINGS lazy_load_tables = 1;

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_replicated (n UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/{table}', 'r1') ORDER BY n;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_local (n UInt64) ENGINE = MergeTree ORDER BY n;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_sync (n UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/{table}', 'r1') ORDER BY n;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t_replicated SELECT number FROM numbers(10);
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t_local SELECT number FROM numbers(10);

-- Re-attach the database so the tables become unloaded lazy proxies.
DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

-- The `system.tables` filter is spelled with `currentDatabase()` because the style check only
-- recognizes that form. Reading the engine name does not load a lazy table.
USE {CLICKHOUSE_DATABASE_1:Identifier};

SELECT 'listing skips unloaded tables and loads none of them';
SELECT count() FROM system.replicas WHERE database = currentDatabase();
SELECT count() FROM system.parts WHERE database = currentDatabase();
SELECT name, engine = 'TableProxy' AS unloaded FROM system.tables WHERE database = currentDatabase() ORDER BY name;

SELECT 'listing shows a table once it is loaded';
SELECT count() FROM t_replicated;
SELECT count() FROM t_local;
SELECT count() FROM system.replicas WHERE database = currentDatabase();
SELECT count() FROM system.parts WHERE database = currentDatabase() AND active;

SELECT 'a replica command loads the table it names';
SYSTEM SYNC REPLICA t_sync;
SELECT name, engine = 'TableProxy' AS unloaded FROM system.tables WHERE database = currentDatabase() ORDER BY name;

USE {CLICKHOUSE_DATABASE:Identifier};
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
