-- Tags: no-fasttest, no-replicated-database
-- Tag no-fasttest: needs the `s3_plain_rewritable` disk on the local MinIO.
-- A database with `lazy_load_tables` stands every table in for a `StorageTableProxy` until it is first
-- accessed. A table on a read-only disk must be rejected as `TABLE_IS_PERMANENTLY_READ_ONLY` by the
-- statements that write to it even when they are the first thing addressed to the table.

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic SETTINGS lazy_load_tables = 1;

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t (key UInt64, value String) ENGINE = MergeTree ORDER BY key
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1,
    disk = disk(readonly = 1, type = 's3_plain_rewritable', endpoint = 'http://localhost:11111/test/lazy_load_tables_static_storage/',
        access_key_id = clickhouse, secret_access_key = clickhouse);

DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
SELECT 'cold engine', engine FROM system.tables WHERE database = currentDatabase() || '_1' AND name = 't';
DELETE FROM {CLICKHOUSE_DATABASE_1:Identifier}.t WHERE key = 1; -- { serverError TABLE_IS_PERMANENTLY_READ_ONLY }

DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
UPDATE {CLICKHOUSE_DATABASE_1:Identifier}.t SET value = 'x' WHERE key = 1; -- { serverError TABLE_IS_PERMANENTLY_READ_ONLY }

DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t DELETE WHERE key = 1; -- { serverError TABLE_IS_PERMANENTLY_READ_ONLY }

DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
TRUNCATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t; -- { serverError TABLE_IS_PERMANENTLY_READ_ONLY }

SELECT 'loaded engine', engine FROM system.tables WHERE database = currentDatabase() || '_1' AND name = 't';

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
