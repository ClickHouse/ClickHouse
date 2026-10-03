-- Tags: shard, no-replicated-database
--       no-replicated-database: `DETACH DATABASE` / `ATTACH DATABASE` of an `Atomic` database
--       with the `lazy_load_tables` setting.

-- https://github.com/ClickHouse/ClickHouse/issues/116090
-- In a database with `lazy_load_tables = 1`, a table is attached as a `StorageTableProxy` around the
-- real storage. The proxy forwards `isRemote` and `readsFromOtherTables`, but not its type, so the
-- `Merge` look-through in `validateCorrelatedSubqueries` - keyed on the concrete `StorageMerge` - did
-- not fire, and the dependency walk found nothing because `Merge` records no referential dependency.
-- A correlated subquery over a lazily loaded `Merge` whose source is a view over a `Distributed`
-- table was planned instead of being refused.

SET enable_analyzer = 1;
SET allow_experimental_correlated_subqueries = 1;

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic SETTINGS lazy_load_tables = 1;

-- Use the database so the view's `AS` clause resolves the table names without an explicit prefix.
USE {CLICKHOUSE_DATABASE_1:Identifier};

CREATE TABLE t05100_local (n UInt32, k UInt32, v Int64) ENGINE = MergeTree ORDER BY n;
INSERT INTO t05100_local SELECT number % 10, number, number * 10 FROM numbers(100);
CREATE TABLE t05100_dist AS t05100_local ENGINE = Distributed(test_shard_localhost, {CLICKHOUSE_DATABASE_1:String}, t05100_local);
CREATE VIEW v05100_remote AS SELECT * FROM t05100_dist;
CREATE TABLE m05100_over_view AS t05100_local ENGINE = Merge({CLICKHOUSE_DATABASE_1:String}, '^v05100_remote$');
CREATE TABLE m05100_over_local AS t05100_local ENGINE = Merge({CLICKHOUSE_DATABASE_1:String}, '^t05100_local$');

-- Re-attach the database so the tables become unloaded lazy proxies.
DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

USE {CLICKHOUSE_DATABASE_1:Identifier};

-- Prove both `Merge` tables are still unloaded proxies at the time of the queries below. The
-- `system.tables` filter is spelled with `currentDatabase()` because the style check only recognizes
-- that form; reading the engine name does not load a lazy table.
SELECT name, engine FROM system.tables WHERE database = currentDatabase() AND name LIKE 'm05100%' ORDER BY name;

SELECT 'through a lazily loaded Merge over the view';
SELECT o.v FROM m05100_over_view AS o WHERE EXISTS (SELECT 1 FROM m05100_over_view AS i WHERE i.n = o.n); -- { serverError NOT_IMPLEMENTED }

SELECT 'a lazily loaded Merge over a local table still works';
SELECT count() FROM (SELECT o.v FROM m05100_over_local AS o WHERE EXISTS (SELECT 1 FROM m05100_over_local AS i WHERE i.n = o.n));

USE {CLICKHOUSE_DATABASE:Identifier};
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
