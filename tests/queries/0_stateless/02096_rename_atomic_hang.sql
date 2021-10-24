-- Tags: no-parallel

drop database if exists db_hang;
drop database if exists db_hang_temp;
create database db_hang engine=Ordinary;
use db_hang;
create table db_hang.test(A Int64) Engine=MergeTree order by A;
create materialized view db_hang.test_mv(A Int64) Engine=MergeTree order by A as select * from db_hang.test;
insert into db_hang.test select * from numbers(1000);

create database db_hang_temp engine=Atomic;
rename table db_hang.test to db_hang_temp.test;
rename table db_hang.test_mv to db_hang_temp.test_mv;

drop database db_hang;
rename database db_hang_temp to db_hang;
insert into db_hang.test select * from numbers(1000);
select count() from db_hang.test;
select count() from db_hang.test_mv;
drop database db_hang;
