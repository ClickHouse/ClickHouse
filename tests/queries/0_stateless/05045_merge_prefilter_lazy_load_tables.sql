-- Tags: shard, no-replicated-database
--       no-replicated-database: `DETACH DATABASE` / `ATTACH DATABASE` of an `Atomic` database
--       with the `lazy_load_tables` setting.

-- In a database with `lazy_load_tables = 1`, an unloaded table is a `StorageTableProxy`.
-- The proxy must forward `readsFromOtherTables` to the nested storage: otherwise a
-- `WHERE _table = ...` filter over a `Merge` table would prune a lazily loaded `Distributed`
-- (or `Merge`, `Buffer`, `Alias`) child by the proxy's own name and silently return no rows
-- after a restart or `ATTACH DATABASE` (found by review in
-- https://github.com/ClickHouse/ClickHouse/pull/116371).

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic SETTINGS lazy_load_tables = 1;

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t05045_leaf (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t05045_leaf VALUES (1), (2), (3);
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t05045_dist (x UInt64) ENGINE = Distributed(test_shard_localhost, {CLICKHOUSE_DATABASE_1:String}, t05045_leaf);

-- Re-attach the database so the tables become unloaded lazy proxies.
DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

-- Prove the child is still an unloaded proxy at the time of the query below.
-- The `system.tables` filter is spelled with `currentDatabase()` rather than the equivalent
-- `{CLICKHOUSE_DATABASE_1:String}` because the style check only recognizes the former; `USE`
-- does not load the lazy tables, so the engine reported below is still the proxy.
USE {CLICKHOUSE_DATABASE_1:Identifier};
SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = 't05045_dist';
USE {CLICKHOUSE_DATABASE:Identifier};

-- The rows of the `Distributed` child carry the leaf's name; the proxy must not be pruned.
SELECT count() FROM merge({CLICKHOUSE_DATABASE_1:String}, '^t05045_dist$') WHERE _table = 't05045_leaf';
SELECT DISTINCT _table FROM merge({CLICKHOUSE_DATABASE_1:String}, '^t05045_dist$');
-- The same at the `FetchColumns` stage (`ARRAY JOIN` prevents forwarding the query to the child):
SELECT count() FROM merge({CLICKHOUSE_DATABASE_1:String}, '^t05045_dist$') ARRAY JOIN [1] AS one WHERE _table = 't05045_leaf';
-- No rows carry the child's own name:
SELECT count() FROM merge({CLICKHOUSE_DATABASE_1:String}, '^t05045_dist$') WHERE _table = 't05045_dist';

-- A lazily loaded `MergeTree` child does not delegate its reads and stays prunable:
-- filtering on another name reads nothing from it.
DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
SELECT count() FROM merge({CLICKHOUSE_DATABASE_1:String}, '^t05045_leaf$') WHERE _table = 't05045_leaf';
SELECT count() FROM merge({CLICKHOUSE_DATABASE_1:String}, '^t05045_leaf$') WHERE _table = 'no_such_table';

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
