-- Tags: zookeeper, no-replicated-database, no-ordinary-database
-- no-replicated-database: we explicitly run this test by creating a replicated database

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE:Identifier};

CREATE DATABASE {CLICKHOUSE_DATABASE:Identifier} ENGINE=Replicated('/clickhouse/databases/{database}', 'shard1', 'replica1') FORMAT NULL;

USE {CLICKHOUSE_DATABASE:Identifier};

CREATE TABLE t1 (x UInt8, y String) ENGINE=ReplicatedMergeTree ORDER BY x FORMAT NULL;

TRUNCATE DATABASE {CLICKHOUSE_DATABASE:Identifier};

-- recreating the same table again shouldn't fail
CREATE TABLE t1 (x UInt8, y String) ENGINE=ReplicatedMergeTree ORDER BY x FORMAT NULL;

DROP DATABASE {CLICKHOUSE_DATABASE:Identifier};
