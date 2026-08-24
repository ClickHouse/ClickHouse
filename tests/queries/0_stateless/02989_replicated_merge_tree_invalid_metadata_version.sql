-- Tags: zookeeper

DROP TABLE IF EXISTS test_table_replicated;
CREATE TABLE test_table_replicated
(
    id UInt64,
    value String
) ENGINE=ReplicatedMergeTree('/clickhouse/tables/{database}/test_table_replicated', '1_replica') ORDER BY id;

ALTER TABLE test_table_replicated ADD COLUMN insert_time DateTime;

SELECT name, version FROM system.zookeeper
WHERE path = (SELECT zookeeper_path FROM system.replicas WHERE database = currentDatabase() AND table = 'test_table_replicated')
AND name = 'metadata' FORMAT Vertical;

DROP TABLE IF EXISTS test_table_replicated_second;
CREATE TABLE test_table_replicated_second
(
    id UInt64,
    value String,
    insert_time DateTime
) ENGINE=ReplicatedMergeTree('/clickhouse/tables/{database}/test_table_replicated', '2_replica') ORDER BY id;

DROP TABLE test_table_replicated;

SELECT '--';

SELECT name, value FROM system.zookeeper
WHERE path = (SELECT replica_path FROM system.replicas WHERE database = currentDatabase() AND table = 'test_table_replicated_second')
AND name = 'metadata_version' FORMAT Vertical;

SYSTEM RESTART REPLICA test_table_replicated_second;

ALTER TABLE test_table_replicated_second ADD COLUMN insert_time_updated DateTime;

SELECT '--';

DESCRIBE test_table_replicated_second;

DROP TABLE test_table_replicated_second;
