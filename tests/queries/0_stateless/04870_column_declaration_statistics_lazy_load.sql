-- Tags: no-replicated-database

-- With `lazy_load_tables = 1`, a re-attached table is a `StorageTableProxy` wrapping the real storage.
-- `AlterCommands::validate` checks `supportsStatistics` (and `supportsTTL`) on the storage the ALTER is
-- addressed to, which is the proxy itself for a lazily loaded table. `StorageProxy` did not forward
-- them, so the column-declaration `STATISTICS(...)` spelling (and a column `TTL`) threw NOT_IMPLEMENTED
-- on a lazily loaded `MergeTree` table, while the dedicated `MODIFY STATISTICS` reached the nested
-- table through `checkAlterIsPossible` — support depended on the database's `lazy_load_tables` setting
-- instead of the storage engine.

SET allow_statistics = 1;

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic SETTINGS lazy_load_tables = 1;

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_lazy (key UInt64, v Float64, s String, d DateTime) ENGINE = MergeTree ORDER BY key;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_lazy_memory (x UInt64) ENGINE = Memory;

DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

USE {CLICKHOUSE_DATABASE_1:Identifier};

-- Pin that both tables are still unloaded proxies at the moment of the ALTER.
SELECT name, engine FROM system.tables WHERE database = currentDatabase() ORDER BY name;

-- One ALTER carrying both gated modifiers: both are validated against the proxy in the same call.
ALTER TABLE t_lazy MODIFY COLUMN v Float64 STATISTICS(tdigest), MODIFY COLUMN s String TTL d + INTERVAL 1 MONTH;
SHOW CREATE TABLE t_lazy;

-- The `ADD COLUMN` spelling through a fresh, unloaded proxy as well.
DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
USE {CLICKHOUSE_DATABASE_1:Identifier};
SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = 't_lazy';
ALTER TABLE t_lazy ADD COLUMN v2 Float64 STATISTICS(uniq);
SHOW CREATE TABLE t_lazy;

-- The proxy forwards the nested answer rather than a blanket `true`: a lazily loaded engine without
-- statistics support is still rejected.
ALTER TABLE t_lazy_memory MODIFY COLUMN x UInt64 STATISTICS(tdigest); -- { serverError NOT_IMPLEMENTED }
ALTER TABLE t_lazy_memory ADD COLUMN y UInt64 STATISTICS(tdigest); -- { serverError NOT_IMPLEMENTED }

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
