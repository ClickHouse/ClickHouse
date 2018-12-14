create database if not exists test;
drop table if exists test.t;
create table test.t (a Int32, b Int32) engine = MergeTree partition by (a,b) order by (a);

insert into test.t values (1, 1);
alter table test.t add column c Int32;

select b from test.t prewhere a < 1000;
select c from test.t where a < 1000;
select c from test.t prewhere a < 1000;

drop table test.t;

