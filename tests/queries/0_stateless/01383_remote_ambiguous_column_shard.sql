-- Tags: shard, no-parallel

DROP DATABASE IF EXISTS test_01383;
CREATE DATABASE test_01383;

create table test_01383.fact (id1 Int64, id2 Int64, value Int64) ENGINE = MergeTree() ORDER BY id1;
create table test_01383.dimension (id1 Int64, name String) ENGINE = MergeTree() ORDER BY id1;
insert into test_01383.fact values (1,2,10),(2,2,10),(3,3,10),(4,3,10);
insert into test_01383.dimension values (1,'name_1'),(2,'name_1'),(3,'name_3'),(4, 'name_4');

SELECT f.id1 AS ID, d.name AS Name, sum(f.value) FROM remote('127.0.0.{1,2,3}', test_01383.fact) AS f LEFT JOIN test_01383.dimension AS d ON f.id1 = d.id1 WHERE f.id1 = f.id2 GROUP BY ID, Name ORDER BY ID;

DROP DATABASE test_01383;
