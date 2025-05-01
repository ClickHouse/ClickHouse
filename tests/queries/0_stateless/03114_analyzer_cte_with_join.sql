-- Tags: no-replicated-database
-- https://github.com/ClickHouse/ClickHouse/issues/58500

SET enable_analyzer=1;

drop table if exists t;

create table t  (ID UInt8) Engine= Memory() ;
insert into t values(1),(2),(3);

with a as (select 1 as column_a) , b as (select 2 as column_b)
  select * FROM remote('127.0.0.{1,2}', currentDatabase(), t) as c
  inner join a on ID=column_a inner join b on ID=column_b;

drop table if exists t;
