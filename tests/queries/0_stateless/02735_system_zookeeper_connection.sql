-- Tags: no-fasttest, no-replicated-database, no-shared-merge-tree
-- no-shared-merge-tree -- smt doesn't support aux zookeepers

DROP TABLE IF EXISTS test_zk_connection_table;

CREATE TABLE test_zk_connection_table (
    key UInt64
)
ENGINE ReplicatedMergeTree('zookeeper2:/clickhouse/{database}/02731_zk_connection/{shard}', '{replica}')
ORDER BY tuple();

SET session_timezone = 'UTC';

-- NOTE: Durind the query execution, now() can be evaluated a bit earlier than connected_time
select name, host, port, index, is_expired, keeper_api_version, (connected_time between yesterday() and now() + interval 3 seconds),
       (abs(session_uptime_elapsed_seconds  - zookeeperSessionUptime()) < 10), enabled_feature_flags
from system.zookeeper_connection where name='default';

-- keeper_api_version will by 0 for auxiliary_zookeeper2, because we fail to get /api_version due to chroot
-- I'm not sure if it's a bug or a useful trick to fallback to basic api
-- Also, auxiliary zookeeper is created lazily
select name, host, port, index, is_expired, keeper_api_version, (connected_time between yesterday() and now() + interval 3 seconds)
from system.zookeeper_connection where name!='default';

DROP TABLE IF EXISTS test_zk_connection_table;
