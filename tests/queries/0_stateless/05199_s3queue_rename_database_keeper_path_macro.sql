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

DROP DATABASE {CLICKHOUSE_DATABASE_2:Identifier};
