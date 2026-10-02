-- Tags: no-replicated-database, no-shared-merge-tree
-- no-replicated-database: the database engine is replaced, which drops the `lazy_load_tables` setting.
-- no-shared-merge-tree: the table engine is replaced, and it keeps parts elsewhere.

-- With `lazy_load_tables` the catalog holds a stand-in for a table until the table is first accessed.
-- A stand-in is not a `MergeTreeData`, and the stand-in keeps wrapping the table after it is loaded,
-- so the `MergeTree` `SYSTEM` commands refused a lazily loaded table as not a `MergeTree` table, and
-- `system.parts` missed it for as long as the server ran.

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic SETTINGS lazy_load_tables = 1;

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t (a UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t VALUES (1), (2);

-- A stand-in appears when the database is loaded, so every round below starts from a re-attach.
DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
USE {CLICKHOUSE_DATABASE_1:Identifier};

SELECT 'stand-in', engine FROM system.tables WHERE database = currentDatabase() AND name = 't';
-- Reading a system table only inspects tables and must not load them.
SELECT 'in system.parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't';
SELECT 'in system.parts_columns', count() FROM system.parts_columns WHERE database = currentDatabase() AND table = 't';
-- The server-wide form only inspects tables as well.
SYSTEM UNLOAD PRIMARY KEY;
SELECT 'still stand-in', engine FROM system.tables WHERE database = currentDatabase() AND name = 't';

-- A `SYSTEM` command addressed to the table is an access to it, so it resolves the stand-in instead of
-- refusing the table as not a `MergeTree` table.
SYSTEM WAIT LOADING PARTS t;
SELECT 'loaded', engine FROM system.tables WHERE database = currentDatabase() AND name = 't';
SELECT 'in system.parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't' AND active;
SELECT 'in system.parts_columns', count() FROM system.parts_columns WHERE database = currentDatabase() AND table = 't' AND active;

DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
USE {CLICKHOUSE_DATABASE_1:Identifier};

SYSTEM LOAD PRIMARY KEY t;
SELECT 'loaded', engine FROM system.tables WHERE database = currentDatabase() AND name = 't';

DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
USE {CLICKHOUSE_DATABASE_1:Identifier};

SYSTEM PREWARM MARK CACHE t;
SELECT 'loaded', engine FROM system.tables WHERE database = currentDatabase() AND name = 't';

DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
USE {CLICKHOUSE_DATABASE_1:Identifier};

-- The table is resolved and recognized as `MergeTree`; the command is refused only for its selector.
SYSTEM SYNC MERGES t; -- { serverError BAD_ARGUMENTS }
SELECT 'loaded', engine FROM system.tables WHERE database = currentDatabase() AND name = 't';

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
