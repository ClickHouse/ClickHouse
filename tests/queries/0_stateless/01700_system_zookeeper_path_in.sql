DROP TABLE IF EXISTS sample_table;

CREATE TABLE sample_table (
    key UInt64
)
ENGINE ReplicatedMergeTree('/clickhouse/01700_system_zookeeper_path_in', '1')
ORDER BY tuple();

SELECT name FROM system.zookeeper WHERE path = '/clickhouse/01700_system_zookeeper_path_in' AND name like 'block%' ORDER BY name;
SELECT name FROM system.zookeeper WHERE path = '/clickhouse/01700_system_zookeeper_path_in/replicas' ORDER BY name;
SELECT '========';
SELECT name FROM system.zookeeper WHERE path IN ('/clickhouse/01700_system_zookeeper_path_in') AND name LIKE 'block%' ORDER BY name;
SELECT name FROM system.zookeeper WHERE path IN ('/clickhouse/01700_system_zookeeper_path_in/replicas') ORDER BY name;
SELECT '========';
SELECT name FROM system.zookeeper WHERE path IN ('/clickhouse/01700_system_zookeeper_path_in','/clickhouse/01700_system_zookeeper_path_in/replicas') AND name LIKE 'block%' ORDER BY name;
SELECT '========';
SELECT name FROM system.zookeeper WHERE path IN (SELECT concat('/clickhouse/01700_system_zookeeper_path_in/', name) FROM system.zookeeper WHERE (path = '/clickhouse/01700_system_zookeeper_path_in')) ORDER BY name;

DROP TABLE IF EXISTS sample_table;
