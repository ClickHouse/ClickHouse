-- Tags: system_zookeeper_connection table

DROP TABLE IF EXISTS test_zk_connection_table;

CREATE TABLE test_zk_connection_table (
    key UInt64
)
ENGINE ReplicatedMergeTree('/clickhouse/{database}/02731_zookeeper_connection/{shard}', '{replica}')
ORDER BY tuple();

select * from system.zookeeper_connection;

DROP TABLE IF EXISTS test_zk_connection_table;
