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

DROP DATABASE {CLICKHOUSE_DATABASE:Identifier};
