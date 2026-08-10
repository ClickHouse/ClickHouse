SET send_logs_level = 'fatal';

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE:Identifier};
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};

SET allow_deprecated_database_ordinary = 1;
-- Creation of a database with Ordinary engine emits a warning.
CREATE DATABASE {CLICKHOUSE_DATABASE:Identifier} ENGINE = Ordinary;
SET allow_deprecated_database_ordinary = 0;
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic;

CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.local (x UInt32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.dist (x UInt32)
    ENGINE = Distributed(test_shard_localhost, {CLICKHOUSE_DATABASE:String}, local);

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.local (x UInt32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.dist (x UInt32)
    ENGINE = Distributed(test_shard_localhost, {CLICKHOUSE_DATABASE_1:String}, local);

SELECT 'ordinary rename refused';
RENAME TABLE {CLICKHOUSE_DATABASE:Identifier}.dist TO {CLICKHOUSE_DATABASE:Identifier}.dist2; -- { serverError NOT_IMPLEMENTED }

SELECT 'ordinary to atomic refused';
RENAME TABLE {CLICKHOUSE_DATABASE:Identifier}.dist TO {CLICKHOUSE_DATABASE_1:Identifier}.dist_moved; -- { serverError NOT_IMPLEMENTED }

SELECT 'atomic to ordinary refused';
RENAME TABLE {CLICKHOUSE_DATABASE_1:Identifier}.dist TO {CLICKHOUSE_DATABASE:Identifier}.dist_moved; -- { serverError NOT_IMPLEMENTED }

-- An Atomic database keys the data directory by UUID, so nothing moves and the rename is allowed.
SELECT 'atomic rename allowed';
RENAME TABLE {CLICKHOUSE_DATABASE_1:Identifier}.dist TO {CLICKHOUSE_DATABASE_1:Identifier}.dist2;
SELECT name FROM system.tables WHERE database = concat(currentDatabase(), '_1') ORDER BY name;

-- The refusal is about the queue directory, not about Distributed tables in general: a table
-- function target has no data path.
SELECT 'no data path is not refused';
CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.no_path AS remote('127.0.0.1', system.one);
RENAME TABLE {CLICKHOUSE_DATABASE:Identifier}.no_path TO {CLICKHOUSE_DATABASE:Identifier}.no_path2;
SELECT name FROM system.tables WHERE database = currentDatabase() ORDER BY name;

DROP DATABASE {CLICKHOUSE_DATABASE:Identifier};
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
