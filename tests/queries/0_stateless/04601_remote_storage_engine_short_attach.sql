-- Tags: shard

-- Regression: a detached `Remote` storage engine table with explicit columns over a local-shard
-- table-function target must be re-attachable with a short `ATTACH TABLE t` even when the target's
-- source table is gone. The short form reads the definition back from the metadata stored on this
-- server (it cannot carry a fresh definition), so it must trust the persisted columns instead of
-- re-analyzing the table-function target (which would throw because the source is gone).
--
-- Before the fix, a short `ATTACH` reached `StorageFactory` with `LoadingStrictnessLevel::ATTACH`,
-- which `isLoadingFromExistingMetadata` does not cover, so the table-function target was re-analyzed
-- and re-attaching a valid table failed.

DROP TABLE IF EXISTS remote_short_attach_src SYNC;
DROP TABLE IF EXISTS remote_short_attach_t SYNC;

CREATE TABLE remote_short_attach_src (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO remote_short_attach_src VALUES (42);

-- A local table-function target whose analysis depends on `remote_short_attach_src`.
CREATE TABLE remote_short_attach_t (x UInt64)
    ENGINE = Remote('127.0.0.1', merge(currentDatabase(), '^remote_short_attach_src$'));

SELECT x FROM remote_short_attach_t ORDER BY x;

-- Detach the `Remote` table, then drop the table its target function reads from.
DETACH TABLE remote_short_attach_t;
DROP TABLE remote_short_attach_src SYNC;

-- The short `ATTACH` must succeed even though `remote_short_attach_src` no longer exists.
ATTACH TABLE remote_short_attach_t;

SELECT name, engine, startsWith(engine_full, 'Remote(') FROM system.tables WHERE database = currentDatabase() AND name = 'remote_short_attach_t';

DROP TABLE remote_short_attach_t SYNC;

-- The same holds one level up: `ATTACH DATABASE` loads the tables of a detached database from the
-- metadata stored on this server, so a `Remote` table with explicit columns inside it must attach
-- without re-analyzing its table-function target.
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

CREATE TABLE remote_short_attach_src (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO remote_short_attach_src VALUES (42);

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.remote_short_attach_t (x UInt64)
    ENGINE = Remote('127.0.0.1', merge(currentDatabase(), '^remote_short_attach_src$'));

SELECT x FROM {CLICKHOUSE_DATABASE_1:Identifier}.remote_short_attach_t ORDER BY x;

DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
DROP TABLE remote_short_attach_src SYNC;

-- The `ATTACH DATABASE` must succeed even though `remote_short_attach_src` no longer exists.
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

SELECT name, engine, startsWith(engine_full, 'Remote(') FROM system.tables WHERE database = {CLICKHOUSE_DATABASE_1:String} AND name = 'remote_short_attach_t';

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier} SYNC;
