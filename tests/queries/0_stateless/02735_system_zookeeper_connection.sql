-- Tags: no-fasttest, no-replicated-database

DROP TABLE IF EXISTS test_zk_connection_table;

CREATE TABLE test_zk_connection_table (
    key UInt64
)
ENGINE ReplicatedMergeTree('zookeeper2:/clickhouse/{database}/02731_zk_connection/{shard}', '{replica}')
ORDER BY tuple();

select name, host, port, index, is_expired, keeper_api_version from system.zookeeper_connection order by name;

DROP TABLE IF EXISTS test_zk_connection_table;
