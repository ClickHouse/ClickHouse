-- Tags: shard, no-replicated-database
--       no-replicated-database: `DETACH DATABASE` / `ATTACH DATABASE` of an `Atomic` database
--       with the `lazy_load_tables` setting.

-- In a database with `lazy_load_tables = 1`, an unloaded table is a `StorageTableProxy`.
-- A `WHERE _table = ...` filter over a `Merge` table must select the rows of a lazily loaded
-- `Distributed` child by the child's own name after a restart or `ATTACH DATABASE`, the same as
-- for a loaded child (the case was found by review in
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

-- The rows of the `Distributed` child carry the child's own name, not the leaf's name.
SELECT count() FROM merge({CLICKHOUSE_DATABASE_1:String}, '^t05045_dist$') WHERE _table = 't05045_dist';
SELECT DISTINCT _table FROM merge({CLICKHOUSE_DATABASE_1:String}, '^t05045_dist$');
-- The same at the `FetchColumns` stage (`ARRAY JOIN` prevents forwarding the query to the child):
SELECT count() FROM merge({CLICKHOUSE_DATABASE_1:String}, '^t05045_dist$') ARRAY JOIN [1] AS one WHERE _table = 't05045_dist';
-- No rows carry the leaf's name:
SELECT count() FROM merge({CLICKHOUSE_DATABASE_1:String}, '^t05045_dist$') WHERE _table = 't05045_leaf';

-- A lazily loaded `MergeTree` child stays prunable:
-- filtering on another name reads nothing from it.
DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
SELECT count() FROM merge({CLICKHOUSE_DATABASE_1:String}, '^t05045_leaf$') WHERE _table = 't05045_leaf';
SELECT count() FROM merge({CLICKHOUSE_DATABASE_1:String}, '^t05045_leaf$') WHERE _table = 'no_such_table';

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
