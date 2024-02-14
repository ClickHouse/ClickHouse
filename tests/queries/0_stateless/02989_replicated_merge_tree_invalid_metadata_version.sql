-- Tags: zookeeper

DROP TABLE IF EXISTS test_table_replicated;
CREATE TABLE test_table_replicated
(
    id UInt64,
    value String
) ENGINE=ReplicatedMergeTree('/clickhouse/tables/{database}/test_table_replicated', '1_replica') ORDER BY id;

ALTER TABLE test_table_replicated ADD COLUMN insert_time DateTime;

DROP TABLE test_table_replicated;

CREATE TABLE test_table_replicated
(
    id UInt64,
    value String,
    insert_time DateTime
) ENGINE=ReplicatedMergeTree('/clickhouse/tables/{database}/test_table_replicated', '2_replica') ORDER BY id;

SELECT name, value FROM system.zookeeper
WHERE path = '/clickhouse/tables/' || currentDatabase() ||'/test_table_replicated/replicas/2_replica'
AND name = 'metadata_version' FORMAT Vertical;

SYSTEM RESTART REPLICA test_table_replicated;

ALTER TABLE test_table_replicated ADD COLUMN insert_time_updated DateTime;

SELECT '--';

DESCRIBE test_table_replicated;

DROP TABLE test_table_replicated;
