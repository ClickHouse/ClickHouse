DROP TABLE IF EXISTS test_zk_connection_table;

CREATE TABLE test_zk_connection_table (
    key UInt64
)
ENGINE ReplicatedMergeTree('/clickhouse/{database}/02731_zk_connection/{shard}', '{replica}')
ORDER BY tuple();

select host, port, is_expired from system.zookeeper_connection where name='default_zookeeper';

DROP TABLE IF EXISTS test_zk_connection_table;
