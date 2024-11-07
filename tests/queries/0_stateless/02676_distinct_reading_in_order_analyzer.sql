drop table if exists t;

set enable_analyzer=1;

create table t (a UInt64, b UInt64) engine=MergeTree() order by (a);
insert into t select number % 2, number from numbers(10);

set optimize_distinct_in_order=1;
select trimBoth(explain) from (explain pipeline select distinct a from t) where explain like '%InOrder%';
