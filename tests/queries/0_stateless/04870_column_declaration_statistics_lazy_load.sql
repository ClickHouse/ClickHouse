-- Tags: no-replicated-database

-- With `lazy_load_tables = 1`, a re-attached table is a `StorageTableProxy` wrapping the real storage.
-- `AlterCommands::validate` checks `supportsTTL` on the storage the ALTER is addressed to, which is the
-- proxy itself for a lazily loaded table. `StorageProxy` did not forward it, so a column `TTL` threw
-- NOT_IMPLEMENTED on a lazily loaded `MergeTree` table — support depended on the database's
-- `lazy_load_tables` setting instead of the storage engine.

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic SETTINGS lazy_load_tables = 1;

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_lazy (key UInt64, v Float64, s String, d DateTime) ENGINE = MergeTree ORDER BY key;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_lazy_log (x UInt64) ENGINE = Log;

DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

USE {CLICKHOUSE_DATABASE_1:Identifier};

-- Pin that both tables are still unloaded proxies at the moment of the ALTER.
SELECT name, engine FROM system.tables WHERE database = currentDatabase() ORDER BY name;

ALTER TABLE t_lazy MODIFY COLUMN s String TTL d + INTERVAL 1 MONTH;
SHOW CREATE TABLE t_lazy;

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
