drop table if exists t1;
drop table if exists t2;
drop table if exists meterialized_table_storage;
drop table if exists meterialized_table;

create table t1
(
    day Date,
    id UInt64
) Engine = MergeTree order by tuple();

create table t2
(
    id UInt64
) Engine = MergeTree order by tuple();

CREATE TABLE meterialized_table_storage
(
    `day` Date,
    `id` UInt32
) Engine = MergeTree order by tuple();

CREATE MATERIALIZED VIEW meterialized_table TO meterialized_table_storage AS
SELECT
    A.day as day ,
    B.id as id
FROM t1 AS A
INNER JOIN t2 AS B ON A.id = B.id;

SHOW CREATE TABLE meterialized_table;

drop table t1;
drop table t2;
drop table meterialized_table_storage;
drop table meterialized_table;
