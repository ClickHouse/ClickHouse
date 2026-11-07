-- Tags: no-fasttest, zookeeper
-- no-fasttest: S3Queue is not built in the fast test.
-- zookeeper: the queue keeps its metadata in Keeper.

-- A detached table has no storage object, but its metadata moves with the database and `ATTACH` loads it
-- under the new name, so `RENAME DATABASE` answers for it from its `CREATE` query.
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic;

-- `{default_path_test}` holds `{database}` and reaches metadata intact.
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.bound (a UInt32)
ENGINE = S3Queue('http://whatever-we-dont-care:9001/root/data/', 'u', 'p', CSV)
SETTINGS mode = 'unordered', keeper_path = '/05359{default_path_test}bound';

-- No `keeper_path`: the path is built from the database UUID.
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.free (a UInt32)
ENGINE = S3Queue('http://whatever-we-dont-care:9001/root/data/', 'u', 'p', CSV)
SETTINGS mode = 'unordered';

-- A direct `{database}` is unfolded by CREATE, so the stored path holds the name literally.
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.unfolded (a UInt32)
ENGINE = S3Queue('http://whatever-we-dont-care:9001/root/data/', 'u', 'p', CSV)
SETTINGS mode = 'unordered', keeper_path = '/05359/{database}/unfolded';

DETACH TABLE {CLICKHOUSE_DATABASE_1:Identifier}.bound;
DETACH TABLE {CLICKHOUSE_DATABASE_1:Identifier}.free;
DETACH TABLE {CLICKHOUSE_DATABASE_1:Identifier}.unfolded;

RENAME DATABASE {CLICKHOUSE_DATABASE_1:Identifier} TO {CLICKHOUSE_DATABASE_2:Identifier}; -- { serverError NOT_IMPLEMENTED }

ATTACH TABLE {CLICKHOUSE_DATABASE_1:Identifier}.bound;
DETACH TABLE {CLICKHOUSE_DATABASE_1:Identifier}.bound PERMANENTLY;

RENAME DATABASE {CLICKHOUSE_DATABASE_1:Identifier} TO {CLICKHOUSE_DATABASE_2:Identifier}; -- { serverError NOT_IMPLEMENTED }

-- The refusals land before anything is moved.
SELECT table, is_permanently FROM system.detached_tables WHERE database = {CLICKHOUSE_DATABASE_1:String} ORDER BY table;

-- Without the bound table the same rename succeeds, and the other two attach under the new name.
ATTACH TABLE {CLICKHOUSE_DATABASE_1:Identifier}.bound;
DROP TABLE {CLICKHOUSE_DATABASE_1:Identifier}.bound;
RENAME DATABASE {CLICKHOUSE_DATABASE_1:Identifier} TO {CLICKHOUSE_DATABASE_2:Identifier};
ATTACH TABLE {CLICKHOUSE_DATABASE_2:Identifier}.free;
ATTACH TABLE {CLICKHOUSE_DATABASE_2:Identifier}.unfolded;
SELECT name FROM system.tables WHERE database = {CLICKHOUSE_DATABASE_2:String} ORDER BY name;

DROP DATABASE {CLICKHOUSE_DATABASE_2:Identifier};

-- An untouched lazy stand-in whose path does not depend on the database name is allowed, and is not loaded.
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic SETTINGS lazy_load_tables = 1;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.lazy_free (a UInt32)
ENGINE = S3Queue('http://whatever-we-dont-care:9001/root/data/', 'u', 'p', CSV)
SETTINGS mode = 'unordered';

DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
USE {CLICKHOUSE_DATABASE_1:Identifier};
SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = 'lazy_free';

RENAME DATABASE {CLICKHOUSE_DATABASE_1:Identifier} TO {CLICKHOUSE_DATABASE_2:Identifier};
USE {CLICKHOUSE_DATABASE_2:Identifier};
SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = 'lazy_free';

-- A stand-in is loaded under the name it was attached with, so reattach it under the new one before the drop loads it.
DETACH DATABASE {CLICKHOUSE_DATABASE_2:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_2:Identifier};
DROP DATABASE {CLICKHOUSE_DATABASE_2:Identifier};

-- `AzureQueue` is checked the same way, as an untouched stand-in and as a detached table.
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic SETTINGS lazy_load_tables = 1;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.azure_lazy (a UInt32)
ENGINE = AzureQueue('DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://whatever-we-dont-care:10000/devstoreaccount1/;', 'cont', '*', 'CSV')
SETTINGS mode = 'unordered', keeper_path = '/05359{default_path_test}azure_lazy';

DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
USE {CLICKHOUSE_DATABASE_1:Identifier};
SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = 'azure_lazy';

RENAME DATABASE {CLICKHOUSE_DATABASE_1:Identifier} TO {CLICKHOUSE_DATABASE_2:Identifier}; -- { serverError NOT_IMPLEMENTED }
SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = 'azure_lazy';

DROP TABLE {CLICKHOUSE_DATABASE_1:Identifier}.azure_lazy;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.azure_detached (a UInt32)
ENGINE = AzureQueue('DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://whatever-we-dont-care:10000/devstoreaccount1/;', 'cont', '*', 'CSV')
SETTINGS mode = 'unordered', keeper_path = '/05359{default_path_test}azure_detached';
DETACH TABLE {CLICKHOUSE_DATABASE_1:Identifier}.azure_detached PERMANENTLY;
RENAME DATABASE {CLICKHOUSE_DATABASE_1:Identifier} TO {CLICKHOUSE_DATABASE_2:Identifier}; -- { serverError NOT_IMPLEMENTED }

ATTACH TABLE {CLICKHOUSE_DATABASE_1:Identifier}.azure_detached;
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
