-- Tags: no-replicated-database
-- A database with `lazy_load_tables` stands every table in for a `StorageTableProxy` until it is
-- first accessed. Once the table is loaded, the database hands out the storage itself, so everything
-- that recognizes a table by its concrete type works again.

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic SETTINGS lazy_load_tables = 1;

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t (id UInt64, s String) ENGINE = MergeTree ORDER BY id;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd');

DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

-- `USE` only so that the queries below can say `currentDatabase()`.
USE {CLICKHOUSE_DATABASE_1:Identifier};

-- Nothing has touched the table yet, so it is still only a proxy.
SELECT 'cold engine', engine FROM system.tables WHERE database = currentDatabase() AND name = 't';

-- Reading the table loads it, and the database then replaces the proxy with the loaded storage.
SELECT 'select', count() FROM {CLICKHOUSE_DATABASE_1:Identifier}.t;

SELECT 'engine', engine FROM system.tables WHERE database = currentDatabase() AND name = 't';
SELECT 'parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't' AND active;
SELECT 'parts_columns', count() > 0 FROM system.parts_columns WHERE database = currentDatabase() AND table = 't' AND active;

-- A mutation and a lightweight delete both need the real storage.
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t DELETE WHERE id = 1 SETTINGS mutations_sync = 2;
SELECT 'after alter delete', count() FROM {CLICKHOUSE_DATABASE_1:Identifier}.t;

DELETE FROM {CLICKHOUSE_DATABASE_1:Identifier}.t WHERE id = 2;
SELECT 'after lightweight delete', count() FROM {CLICKHOUSE_DATABASE_1:Identifier}.t;

-- The same statements must also work as the very first thing addressed to a table that has not been
-- accessed at all since the database was attached.
DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

DELETE FROM {CLICKHOUSE_DATABASE_1:Identifier}.t WHERE id = 3;
SELECT 'cold lightweight delete', count() FROM {CLICKHOUSE_DATABASE_1:Identifier}.t;

DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t DELETE WHERE id = 4 SETTINGS mutations_sync = 2;
SELECT 'cold alter delete', count() FROM {CLICKHOUSE_DATABASE_1:Identifier}.t;

-- An action lock (`SYSTEM STOP MERGES` and friends) is keyed by the storage it was taken on, so one
-- taken while the table was still a proxy has to survive the replacement: the matching
-- `SYSTEM START MERGES` addresses the storage that took the proxy's place.
-- `max_bytes_to_merge_at_max_space_in_pool` keeps background merges away, so the two parts stay two
-- until the explicit `OPTIMIZE`, which ignores that limit for `FINAL`.
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t2 (id UInt64) ENGINE = MergeTree ORDER BY id
    SETTINGS max_bytes_to_merge_at_max_space_in_pool = 1;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t2 VALUES (1);
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t2 VALUES (2);

DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
USE {CLICKHOUSE_DATABASE_1:Identifier};

SYSTEM STOP MERGES {CLICKHOUSE_DATABASE_1:Identifier}.t2;
-- Reading `system.parts` replaces the proxy, so the `SYSTEM START MERGES` below no longer addresses
-- the storage the lock was taken on.
SELECT 'stopped merges parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't2' AND active;
SYSTEM START MERGES {CLICKHOUSE_DATABASE_1:Identifier}.t2;
OPTIMIZE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t2 FINAL;
SELECT 'started merges parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't2' AND active;

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
