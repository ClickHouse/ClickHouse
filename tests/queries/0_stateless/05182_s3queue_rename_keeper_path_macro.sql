-- Tags: no-fasttest, zookeeper, no-ordinary-database, no-replicated-database
-- Tag no-ordinary-database: without a UUID the queue metadata registry is keyed by table name, so
-- arm 2's allowed rename would leave a stale registration and the following DROP would abort.

-- `{default_name_test}` is `table_{table}`. The CREATE-time pass expands special macros only, so a
-- user macro reaches metadata intact and the Keeper path is re-derived from the table name on load.
CREATE TABLE q (a UInt32)
ENGINE = S3Queue('http://whatever-we-dont-care:9001/root/data/', 'u', 'p', CSV)
SETTINGS mode = 'unordered', keeper_path = '/05182/{database}/{default_name_test}';

-- The resolved path really does end in the table name, so the rename below would move it.
SELECT value = '/05182/' || currentDatabase() || '/table_q' FROM system.s3_queue_settings
WHERE database = currentDatabase() AND table = 'q' AND name = 'keeper_path';

RENAME TABLE q TO q_renamed; -- { serverError NOT_IMPLEMENTED }

-- `{default_path_test}` is `/clickhouse/tables/{database}/{shard}/`, so this path depends on the
-- database name but not on the table name.
CREATE TABLE r (a UInt32)
ENGINE = S3Queue('http://whatever-we-dont-care:9001/root/data/', 'u', 'p', CSV)
SETTINGS mode = 'unordered', keeper_path = '/05182{default_path_test}fixed';

SELECT position(value, currentDatabase()) > 0 FROM system.s3_queue_settings
WHERE database = currentDatabase() AND table = 'r' AND name = 'keeper_path';

-- Renaming inside the database keeps the same Keeper path, so it must still be allowed.
RENAME TABLE r TO r_renamed;
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'r_renamed';

-- Moving that table to another database would change what `{database}` expands to.
CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
RENAME TABLE r_renamed TO {CLICKHOUSE_DATABASE_1:Identifier}.r; -- { serverError NOT_IMPLEMENTED }

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
DROP TABLE q;
DROP TABLE r_renamed;

-- An `Ordinary` database renames through `DatabaseOnDisk`, which never asks the storage, so the
-- refusal has to come from `rename()` itself.
SET allow_deprecated_database_ordinary = 1;
CREATE DATABASE {CLICKHOUSE_DATABASE_2:Identifier} ENGINE = Ordinary;
CREATE TABLE {CLICKHOUSE_DATABASE_2:Identifier}.o (a UInt32)
ENGINE = S3Queue('http://whatever-we-dont-care:9001/root/data/', 'u', 'p', CSV)
SETTINGS mode = 'unordered', keeper_path = '/05182/{database}/{default_name_test}';

RENAME TABLE {CLICKHOUSE_DATABASE_2:Identifier}.o TO {CLICKHOUSE_DATABASE_2:Identifier}.o2; -- { serverError NOT_IMPLEMENTED }

-- The refusal lands after `DatabaseOnDisk` has already detached the table, so prove the rollback
-- re-attached it rather than leaving it unusable.
SELECT count() FROM system.tables WHERE database = {CLICKHOUSE_DATABASE_2:String} AND name = 'o';

DROP DATABASE {CLICKHOUSE_DATABASE_2:Identifier};
