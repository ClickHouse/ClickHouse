-- Tags: zookeeper, no-replicated-database, no-ordinary-database, no-shared-catalog
-- no-replicated-database: the test creates its own Replicated database to exercise the metadata-transaction path.

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Replicated('/test/04328/{database}', '{shard}', '{replica}');

SET distributed_ddl_output_mode = 'none';

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.source (id UInt64, status Enum8('A' = 0, 'B' = 1)) ENGINE = MergeTree ORDER BY id;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.target (id UInt64, status Enum8('A' = 0, 'B' = 1), cnt UInt64) ENGINE = SummingMergeTree ORDER BY (id, status);
CREATE MATERIALIZED VIEW {CLICKHOUSE_DATABASE_1:Identifier}.mv TO {CLICKHOUSE_DATABASE_1:Identifier}.target AS
    SELECT id, status, count() AS cnt FROM {CLICKHOUSE_DATABASE_1:Identifier}.source GROUP BY id, status;

-- An upstream ALTER on a Replicated database must not crash the server while refreshing the view's
-- inferred columns (a second persisted alter inside the ALTER's committed metadata transaction used to
-- raise "Cannot add ZooKeeper operation because query is executed").
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.source MODIFY COLUMN status Enum8('A' = 0, 'B' = 1, 'C' = 2);
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.target MODIFY COLUMN status Enum8('A' = 0, 'B' = 1, 'C' = 2);

SELECT '-- mv reports the new enum after the upstream alter';
USE {CLICKHOUSE_DATABASE_1:Identifier};
SELECT name, type FROM system.columns
WHERE database = currentDatabase() AND table = 'mv' ORDER BY position;

SELECT '-- reading the mv with the new enum value works';
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.source SETTINGS async_insert = 0 VALUES (1, 'C');
SELECT * FROM {CLICKHOUSE_DATABASE_1:Identifier}.mv WHERE id = 1 ORDER BY id;

USE {CLICKHOUSE_DATABASE:Identifier};
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
