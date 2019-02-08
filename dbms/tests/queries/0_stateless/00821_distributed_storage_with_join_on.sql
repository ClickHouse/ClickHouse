DROP TABLE IF EXISTS test.table1;
DROP TABLE IF EXISTS test.table2;

CREATE TABLE test.table1 AS system.columns ENGINE = Distributed('test_shard_localhost', system, columns);
CREATE TABLE test.table2 AS system.tables ENGINE = Distributed('test_shard_localhost', system, tables);

SELECT 1 FROM test.table1 T1 ALL INNER JOIN test.table2 T2 ON T1.table = T2.name LIMIT 1;
SELECT 1 FROM cluster('test_shard_localhost', system.columns) T1 ALL INNER JOIN cluster('test_shard_localhost', system.tables) T2 ON T1.table = T2.name LIMIT 1;
SELECT 1 FROM (SELECT * FROM test.table1) T1 ALL INNER JOIN (SELECT * FROM test.table2) T2 ON T1.table = T2.name LIMIT 1;

DROP TABLE IF EXISTS test.table1;
DROP TABLE IF EXISTS test.table2;
