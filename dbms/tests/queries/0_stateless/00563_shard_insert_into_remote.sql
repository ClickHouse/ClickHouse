create database if not exists test;
drop table if exists test.tab;
create table test.tab (val UInt8) engine = MergeTree order by val;
insert into function remote('127.0.0.2', test.tab) values (1);
insert into function remote('127.0.0.{2|3}', test.tab) values (2);
insert into function remote('127.0.0.{2|3|4}', test.tab) values (3);
select * from test.tab order by val;

