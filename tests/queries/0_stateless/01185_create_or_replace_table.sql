drop database if exists test_01185;
create database test_01185 engine=Atomic;

replace table test_01185.t1 (n UInt64, s String) engine=MergeTree order by n; -- { serverError 60 }
show tables from test_01185;
create or replace table test_01185.t1 (n UInt64, s String) engine=MergeTree order by n;
show tables from test_01185;
show create table test_01185.t1;

insert into test_01185.t1 values (1, 'test');
create or replace table test_01185.t1 (n UInt64, s Nullable(String)) engine=MergeTree order by n;
insert into test_01185.t1 values (2, null);
show tables from test_01185;
show create table test_01185.t1;
select * from test_01185.t1;

replace table test_01185.t1 (n UInt64) engine=MergeTree order by n;
insert into test_01185.t1 values (3);
show tables from test_01185;
show create table test_01185.t1;
select * from test_01185.t1;

drop database test_01185;
