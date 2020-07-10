DROP TABLE IF EXISTS fact;
DROP TABLE IF EXISTS dimension;

create table fact (id1 Int64, id2 Int64, value Int64) ENGINE = MergeTree() ORDER BY id1;
create table dimension (id1 Int64, name String) ENGINE = MergeTree() ORDER BY id1;
insert into fact values (1,2,10),(2,2,10),(3,3,10),(4,3,10);
insert into dimension values (1,'name_1'),(2,'name_1'),(3,'name_3'),(4, 'name_4');

SELECT f.id1 AS ID, d.name AS Name, sum(f.value) FROM remote('127.0.0.{1,2,3}', currentDatabase(), fact) AS f LEFT JOIN dimension AS d ON f.id1 = d.id1 WHERE f.id1 = f.id2 GROUP BY ID, Name;

DROP TABLE fact;
DROP TABLE dimension;
