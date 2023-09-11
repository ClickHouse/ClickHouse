-- Tags: no-parallel

DROP DATABASE IF EXISTS test_2900_db;
CREATE DATABASE test_2900_db ENGINE=Memory;

DROP TABLE IF EXISTS test_2900_db.t;

CREATE OR REPLACE VIEW test_2900_db.t (number UInt64) AS SELECT number FROM system.numbers;
SHOW CREATE TABLE test_2900_db.t;

CREATE OR REPLACE VIEW test_2900_db.t AS SELECT number+1 AS next_number FROM system.numbers;
SHOW CREATE TABLE test_2900_db.t;

DROP TABLE test_2900_db.t;

drop table if exists test_2900_db.t1;

create or replace table test_2900_db.t1 (n UInt64, s String) engine=MergeTree order by n;
show tables from test_2900_db;
show create table test_2900_db.t1;

insert into test_2900_db.t1 values (1, 'test');
create or replace table test_2900_db.t1 (n UInt64, s Nullable(String)) engine=MergeTree order by n;
insert into test_2900_db.t1 values (2, null);
show tables from test_2900_db;
show create table test_2900_db.t1;
select * from test_2900_db.t1;

replace table test_2900_db.t1 (n UInt64) engine=MergeTree order by n;
insert into test_2900_db.t1 values (3);
show tables from test_2900_db;
show create table test_2900_db.t1;
select * from test_2900_db.t1;

drop table test_2900_db.t1;

drop database test_2900_db;
