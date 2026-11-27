-- Tags: no-fasttest, zookeeper
-- no-fasttest: S3Queue is not built in the fast test.
-- zookeeper: the queue keeps its metadata in Keeper.

CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

-- `{default_path_test}` is a user macro holding `/clickhouse/tables/{database}/{shard}/`. CREATE expands
-- special macros only, so it reaches metadata intact and the path is re-derived from the database name on load.
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.bound (a UInt32)
ENGINE = S3Queue('http://whatever-we-dont-care:9001/root/data/', 'u', 'p', CSV)
SETTINGS mode = 'unordered', keeper_path = '/05199{default_path_test}bound';

-- No `keeper_path`: the path is built from the database UUID, which a rename does not change.
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.free (a UInt32)
ENGINE = S3Queue('http://whatever-we-dont-care:9001/root/data/', 'u', 'p', CSV)
SETTINGS mode = 'unordered';

-- A direct `{database}` is unfolded by CREATE, so the stored path holds the name literally and the
-- rename cannot move it. Only a macro that survives into metadata binds the path to the name.
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.unfolded (a UInt32)
ENGINE = S3Queue('http://whatever-we-dont-care:9001/root/data/', 'u', 'p', CSV)
SETTINGS mode = 'unordered', keeper_path = '/05199/{database}/unfolded';

-- The resolved path really does hold the database name, so the rename below would move it.
SELECT position(value, {CLICKHOUSE_DATABASE_1:String}) > 0 FROM system.s3_queue_settings
WHERE database = {CLICKHOUSE_DATABASE_1:String} AND table = 'bound' AND name = 'keeper_path';

RENAME DATABASE {CLICKHOUSE_DATABASE_1:Identifier} TO {CLICKHOUSE_DATABASE_2:Identifier}; -- { serverError NOT_IMPLEMENTED }

-- The refusal lands before anything is moved, so both tables are still attached.
EXISTS TABLE {CLICKHOUSE_DATABASE_1:Identifier}.bound;
EXISTS TABLE {CLICKHOUSE_DATABASE_1:Identifier}.free;

-- Without the bound table the same rename succeeds: the refusal belongs to that table, not to the database.
DROP TABLE {CLICKHOUSE_DATABASE_1:Identifier}.bound;
RENAME DATABASE {CLICKHOUSE_DATABASE_1:Identifier} TO {CLICKHOUSE_DATABASE_2:Identifier};
EXISTS TABLE {CLICKHOUSE_DATABASE_2:Identifier}.free;
EXISTS TABLE {CLICKHOUSE_DATABASE_2:Identifier}.unfolded;

DROP DATABASE {CLICKHOUSE_DATABASE_2:Identifier};

-- A lazily loaded queue table is a `StorageTableProxy` until something touches it; once materialized,
-- the proxy must still answer the rename check for the queue behind it.
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic SETTINGS lazy_load_tables = 1;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.lazy_bound (a UInt32)
ENGINE = S3Queue('http://whatever-we-dont-care:9001/root/data/', 'u', 'p', CSV)
SETTINGS mode = 'unordered', keeper_path = '/05199{default_path_test}lazy';

DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
USE {CLICKHOUSE_DATABASE_1:Identifier};

-- Anti-vacuity: it really is a stand-in at this point, so the arm below cannot pass through the
-- ordinary code path by accident.
SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = 'lazy_bound';

-- A settings ALTER goes through `StorageProxy::alter` -> `getNested()`, which materializes the queue.
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.lazy_bound MODIFY SETTING loading_retries = 5;
SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = 'lazy_bound';

RENAME DATABASE {CLICKHOUSE_DATABASE_1:Identifier} TO {CLICKHOUSE_DATABASE_2:Identifier}; -- { serverError NOT_IMPLEMENTED }

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

-- A lazily loaded queue table that nothing has touched has no storage object to ask, so the rename is
-- allowed - and it must not be loaded in order to answer, which is why the forward is not `getNested()`.
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic SETTINGS lazy_load_tables = 1;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.lazy_untouched (a UInt32)
ENGINE = S3Queue('http://whatever-we-dont-care:9001/root/data/', 'u', 'p', CSV)
SETTINGS mode = 'unordered', keeper_path = '/05199{default_path_test}untouched';

DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
USE {CLICKHOUSE_DATABASE_1:Identifier};
SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = 'lazy_untouched';

RENAME DATABASE {CLICKHOUSE_DATABASE_1:Identifier} TO {CLICKHOUSE_DATABASE_2:Identifier};

-- Still a stand-in: answering the check must not have materialized it.
USE {CLICKHOUSE_DATABASE_2:Identifier};
SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = 'lazy_untouched';

DROP DATABASE {CLICKHOUSE_DATABASE_2:Identifier};
