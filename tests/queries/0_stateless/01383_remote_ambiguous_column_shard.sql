-- Tags: shard, no-parallel

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

create table {CLICKHOUSE_DATABASE_1:Identifier}.fact (id1 Int64, id2 Int64, value Int64) ENGINE = MergeTree() ORDER BY id1;
create table {CLICKHOUSE_DATABASE_1:Identifier}.dimension (id1 Int64, name String) ENGINE = MergeTree() ORDER BY id1;
insert into {CLICKHOUSE_DATABASE_1:Identifier}.fact values (1,2,10),(2,2,10),(3,3,10),(4,3,10);
insert into {CLICKHOUSE_DATABASE_1:Identifier}.dimension values (1,'name_1'),(2,'name_1'),(3,'name_3'),(4, 'name_4');

SELECT f.id1 AS ID, d.name AS Name, sum(f.value) FROM remote('127.0.0.{1,2,3}', {CLICKHOUSE_DATABASE_1:Identifier}.fact) AS f LEFT JOIN {CLICKHOUSE_DATABASE_1:Identifier}.dimension AS d ON f.id1 = d.id1 WHERE f.id1 = f.id2 GROUP BY ID, Name ORDER BY ID;

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
