create database if not exists test;
drop table if exists test.tab;
create table test.tab (date Date,  time DateTime, data String) ENGINE = MergeTree(date, (time, data), 8192);
insert into test.tab values ('2018-01-21','2018-01-21 15:12:13','test');
select time FROM remote('127.0.0.{1,2}', test.tab)  WHERE date = '2018-01-21' limit 2;
drop table test.tab;

