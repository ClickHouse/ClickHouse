SELECT name FROM system.zookeeper WHERE path = '/';
SELECT name FROM system.zookeeper WHERE path = 'clickhouse';
SELECT name FROM system.zookeeper WHERE path IN ('/');
SELECT name FROM system.zookeeper WHERE path IN ('clickhouse');
SELECT name FROM system.zookeeper WHERE path IN ('/','/clickhouse');
SELECT name FROM system.zookeeper WHERE path IN (SELECT concat('/clickhouse/',name) FROM system.zookeeper WHERE (path = '/clickhouse/'));