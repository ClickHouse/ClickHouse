DROP TABLE IF EXISTS sample_table;

CREATE TABLE sample_table (
    key UInt64
)
ENGINE ReplicatedMergeTree('/clickhouse/01700_system_zookeeper_path_in/{shard}', '{replica}')
ORDER BY tuple();

SELECT name FROM system.zookeeper WHERE path = '/clickhouse/01700_system_zookeeper_path_in/s1' AND name like 'block%' ORDER BY name;
SELECT name FROM system.zookeeper WHERE path = '/clickhouse/01700_system_zookeeper_path_in/s1/replicas' AND name LIKE '%r1%' ORDER BY name;

SELECT '========';
SELECT name FROM system.zookeeper WHERE path IN ('/clickhouse/01700_system_zookeeper_path_in/s1') AND name LIKE 'block%' ORDER BY name;
SELECT name FROM system.zookeeper WHERE path IN ('/clickhouse/01700_system_zookeeper_path_in/s1/replicas') AND name LIKE '%r1%' ORDER BY name;
SELECT '========';
SELECT name FROM system.zookeeper WHERE path IN ('/clickhouse/01700_system_zookeeper_path_in/s1',
                                                 '/clickhouse/01700_system_zookeeper_path_in/s1/replicas') AND name LIKE 'block%' ORDER BY name;
SELECT '========';
SELECT name FROM system.zookeeper WHERE path IN (SELECT concat('/clickhouse/01700_system_zookeeper_path_in/s1/', name)
    FROM system.zookeeper WHERE (name != 'replicas' AND name NOT LIKE 'leader_election%' AND path = '/clickhouse/01700_system_zookeeper_path_in/s1')) ORDER BY name;

DROP TABLE IF EXISTS sample_table;
