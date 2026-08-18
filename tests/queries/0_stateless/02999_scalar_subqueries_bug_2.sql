drop table if exists source;
drop table if exists target1;
drop table if exists target2;
drop table if exists v_heavy;


create table source(type String) engine=MergeTree order by type;

create view v_heavy as
with nums as (select number from numbers(1e5))
select count(*) n from (select number from numbers(1e5) n1 cross join nums);

create table target1(type String) engine=MergeTree order by type;
create table target2(type String) engine=MergeTree order by type;

set max_execution_time=2;
-- we should not execute scalar subquery here
create materialized view vm_target2 to target2 as select * from source where type='two' and (select sum(sleepEachRow(0.1)) from numbers(30));
