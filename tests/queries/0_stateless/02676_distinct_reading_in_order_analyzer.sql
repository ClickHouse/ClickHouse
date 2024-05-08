drop table if exists t;

set allow_experimental_analyzer=1;

create table t (a UInt64, b UInt64) engine=MergeTree() order by (a);
insert into t select number % 2, number from numbers(10);

select splitByChar(' ', trimBoth(explain))[1] from (explain pipeline select distinct a from t) where explain like '%MergeTreeInOrder%';
