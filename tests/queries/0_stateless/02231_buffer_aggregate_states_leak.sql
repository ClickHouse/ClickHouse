-- Tags: long

drop table if exists buffer_02231;
drop table if exists out_02231;
drop table if exists in_02231;
drop table if exists mv_02231;

-- To reproduce leak of memory tracking of aggregate states,
-- background flush is required.
create table buffer_02231
(
    key Int,
    v1 AggregateFunction(groupArray, String)
) engine=Buffer(currentDatabase(), 'out_02231',
    /* layers= */1,
    /* min/max time  */ 86400, 86400,
    /* min/max rows  */ 1e9, 1e9,
    /* min/max bytes */ 1e12, 1e12,
    /* flush time */    1
);
create table out_02231 as buffer_02231 engine=Null();
create table in_02231 (number Int) engine=Null();

-- Create lots of INSERT blocks with MV
create materialized view mv_02231 to buffer_02231 as select
    number as key,
    groupArrayState(toString(number)) as v1
from in_02231
group by key;

insert into in_02231 select * from numbers(10e6) settings max_memory_usage='300Mi';

drop table buffer_02231;
drop table out_02231;
drop table in_02231;
drop table mv_02231;
