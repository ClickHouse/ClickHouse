create database if not exists test;
drop table if exists t;
create table t (a Int32, b Int32) engine = MergeTree partition by (a,b) order by (a);

insert into t values (1, 1);
alter table t add column c Int32;

select b from t prewhere a < 1000;
select c from t where a < 1000;
select c from t prewhere a < 1000;

drop table t;

