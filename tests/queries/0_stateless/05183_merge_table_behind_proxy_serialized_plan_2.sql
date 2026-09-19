-- Tags: no-replicated-database
--       no-replicated-database: `DETACH DATABASE` / `ATTACH DATABASE` of an `Atomic` database with
--       the `lazy_load_tables` setting.

-- Continuation of 05183_merge_table_behind_proxy_serialized_plan for the one carrier that needs a
-- second database. In a database with `lazy_load_tables = 1` an unloaded table is a
-- `StorageTableProxy`, so a `Merge` table reached through one is routed by `StorageProxy`'s own
-- delegation rather than by any of the concrete `Alias` / `Buffer` / `MaterializedView` classes.

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic SETTINGS lazy_load_tables = 1;

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t05183_src (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t05183_src VALUES (1), (2);
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t05183_merge (x UInt64)
    ENGINE = Merge({CLICKHOUSE_DATABASE_1:Identifier}, '^t05183_src$');

-- Re-attach the database so its tables become unloaded lazy proxies.
DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

-- Switch into the lazy database: it makes the query below address it, and the `system.tables`
-- filter has to be spelled `currentDatabase()` for the style check to recognize it.
USE {CLICKHOUSE_DATABASE_1:Identifier};

-- Prove the `Merge` table is still an unloaded proxy when the query below runs. Only `engine` is
-- read, because it is answered by the proxy itself, while a column such as `data_paths` would
-- materialize the nested storage and defeat the fixture.
SELECT 'engine before', engine FROM system.tables WHERE database = currentDatabase() AND name = 't05183_merge';

SELECT 'lazy proxy', sum(x) FROM remote('127.0.0.2', currentDatabase(), t05183_merge)
SETTINGS enable_analyzer = 1, serialize_query_plan = 1;

USE {CLICKHOUSE_DATABASE:Identifier};
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
