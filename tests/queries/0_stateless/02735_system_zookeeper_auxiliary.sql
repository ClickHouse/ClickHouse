-- Tags: no-fasttest, no-replicated-database, no-shared-merge-tree
-- no-shared-merge-tree -- smt doesn't support aux zookeepers

DROP TABLE IF EXISTS test_system_zookeeper_auxiliary;

CREATE TABLE test_system_zookeeper_auxiliary (
    key UInt64
)
ENGINE ReplicatedMergeTree('zookeeper2:/clickhouse/{database}/02731_test_system_zookeeper_auxiliary/{shard}', '{replica}')
ORDER BY tuple();

SELECT DISTINCT zookeeperName FROM system.zookeeper WHERE path = '/' AND zookeeperName = 'default';
SELECT DISTINCT zookeeperName FROM system.zookeeper WHERE path = '/' AND zookeeperName = 'zookeeper2';

SELECT count() FROM system.zookeeper WHERE path IN '/' AND zookeeperName = 'zookeeper3'; -- { serverError BAD_ARGUMENTS }

SELECT count() = 0 FROM system.zookeeper WHERE path IN '/' AND zookeeperName = 'default' AND zookeeperName = 'zookeeper2';
SELECT count() > 0 FROM system.zookeeper WHERE path IN '/' AND zookeeperName = 'zookeeper2' AND zookeeperName = 'zookeeper2';

DROP TABLE IF EXISTS test_system_zookeeper_auxiliary;
