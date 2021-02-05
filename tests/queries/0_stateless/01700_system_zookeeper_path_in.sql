SELECT name FROM system.zookeeper WHERE path = '/';
SELECT name FROM system.zookeeper WHERE path IN ('/');
SELECT name FROM system.zookeeper WHERE path IN ('/','/clickhouse');