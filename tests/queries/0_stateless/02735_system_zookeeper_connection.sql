-- Tags: no-fasttest, no-replicated-database

DROP TABLE IF EXISTS test_zk_connection_table;

CREATE TABLE test_zk_connection_table (
    key UInt64
)
ENGINE ReplicatedMergeTree('zookeeper2:/clickhouse/{database}/02731_zk_connection/{shard}', '{replica}')
ORDER BY tuple();

-- keeper_api_version will by 0 for auxiliary_zookeeper2, because we fail to get /api_version due to chroot
-- I'm not sure if it's a bug or a useful trick to fallback to basic api
select name, host, port, index, (connected_time between yestedday() and today()), (abs(session_uptime_elapsed_seconds  - zookeeperSessionUptime()) < 10), is_expired, keeper_api_version from system.zookeeper_connection order by name;

DROP TABLE IF EXISTS test_zk_connection_table;
