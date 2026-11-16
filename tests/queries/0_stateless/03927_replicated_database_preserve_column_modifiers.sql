-- Tags: zookeeper, no-replicated-database, no-ordinary-database
-- no-replicated-database: this test explicitly creates a Replicated database.

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE:Identifier};

CREATE DATABASE {CLICKHOUSE_DATABASE:Identifier}
ENGINE = Replicated('/clickhouse/databases/{database}', 'shard1', 'replica1')
FORMAT Null;

USE {CLICKHOUSE_DATABASE:Identifier};

-- Verify that explicit NOT NULL is preserved when data_type_default_nullable=1.
-- The DDL replayed from ZooKeeper on secondary replicas must not re-apply
-- data_type_default_nullable, because the column type is already resolved.
-- The same holds for the columns of ALTER TABLE ... ADD COLUMN and MODIFY COLUMN.
SET data_type_default_nullable = 1;

CREATE TABLE t_not_null
(
    key Int64 NOT NULL,
    value String
)
ENGINE = Memory
FORMAT Null;

SELECT name, type
FROM system.columns
WHERE database = currentDatabase() AND table = 't_not_null'
ORDER BY position;

-- The oldest DDL entry format forwards no settings, so the worker sees only the type resolved on the initiator.
SET distributed_ddl_entry_format_version = 1;
ALTER TABLE t_not_null ADD COLUMN added Int32, ADD COLUMN added_not_null Int32 NOT NULL FORMAT Null;

SELECT name, type
FROM system.columns
WHERE database = currentDatabase() AND table = 't_not_null'
ORDER BY position;

ALTER TABLE t_not_null MODIFY COLUMN added_not_null Int64 FORMAT Null;

SELECT type
FROM system.columns
WHERE database = currentDatabase() AND table = 't_not_null' AND name = 'added_not_null';

DROP DATABASE {CLICKHOUSE_DATABASE:Identifier};
