-- Tags: no-parallel, no-replicated-database, no-shared-merge-tree
-- no-parallel: `SYSTEM DROP REPLICA` reaches server-wide state.
-- no-replicated-database: the database engine is replaced, which drops the `lazy_load_tables` setting.
-- no-shared-merge-tree: the table engine is replaced, and these commands take a different path for it.

-- With `lazy_load_tables` the catalog holds a stand-in for a table until the table is first accessed.
-- A stand-in is not a `ReplicatedMergeTree`, so a command or a system table that identifies a
-- replicated table by casting the catalog object used to report a replicated table as not replicated,
-- and kept doing so for as long as the server ran, even after the table itself was loaded.

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic SETTINGS lazy_load_tables = 1;

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t (a UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/05076_t', 'r1') ORDER BY a;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t VALUES (1);

-- A stand-in appears when the database is loaded, so every round below starts from a re-attach.
DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
USE {CLICKHOUSE_DATABASE_1:Identifier};

SELECT 'stand-in', engine FROM system.tables WHERE database = currentDatabase() AND name = 't';
SELECT 'in system.replicas', count() FROM system.replicas WHERE database = currentDatabase() AND table = 't';

-- A `SYSTEM` command addressed to the table is an access to it, so it resolves the stand-in instead of
-- refusing the table as not replicated.
SYSTEM SYNC REPLICA t;

SELECT 'loaded', engine FROM system.tables WHERE database = currentDatabase() AND name = 't';
-- Reading a system table only inspects tables and never loads them, but it must see the loaded one.
SELECT 'in system.replicas', count() FROM system.replicas WHERE database = currentDatabase() AND table = 't';
SELECT 'in system.replication_queue', count() FROM system.replication_queue WHERE database = currentDatabase() AND table = 't';

DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
USE {CLICKHOUSE_DATABASE_1:Identifier};

-- `SYSTEM DROP REPLICA` resolves the stand-in as well, so the guard against dropping the replica of a
-- live local table applies to a lazily loaded table too.
SYSTEM DROP REPLICA 'r1' FROM TABLE t; -- { serverError TABLE_WAS_NOT_DROPPED }

DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
USE {CLICKHOUSE_DATABASE_1:Identifier};

SYSTEM RESTART REPLICA t;
SELECT 'after restart', count() FROM t;

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
