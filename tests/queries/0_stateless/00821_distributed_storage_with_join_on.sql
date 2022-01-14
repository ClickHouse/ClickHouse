-- NOTE: database = currentDatabase() is not mandatory

DROP TABLE IF EXISTS table1;
DROP TABLE IF EXISTS table2;

CREATE TABLE table1 AS system.columns ENGINE = Distributed('test_shard_localhost', system, columns);
CREATE TABLE table2 AS system.tables ENGINE = Distributed('test_shard_localhost', system, tables);

SELECT 1 FROM table1 T1 ALL INNER JOIN table2 T2 ON T1.table = T2.name LIMIT 1;
SELECT 1 FROM cluster('test_shard_localhost', system.columns) T1 ALL INNER JOIN cluster('test_shard_localhost', system.tables) T2 ON T1.table = T2.name LIMIT 1;
SELECT 1 FROM (SELECT * FROM table1) T1 ALL INNER JOIN (SELECT * FROM table2) T2 ON T1.table = T2.name LIMIT 1;

DROP TABLE IF EXISTS table1;
DROP TABLE IF EXISTS table2;
