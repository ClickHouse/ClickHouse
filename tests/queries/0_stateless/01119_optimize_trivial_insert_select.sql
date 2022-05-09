drop table if exists t;
create table t(n int, a Int64, s String) engine = MergeTree() order by a;

insert into t select 1, sum(number) as c, getSetting('max_threads') from numbers_mt(1000000000);
insert into t select 2, sum(number) as c, getSetting('max_threads') from numbers_mt(1000000000) group by 1;
insert into t select 3, sum(number) as c, getSetting('max_threads') from numbers_mt(100000000) group by 3;
insert into t select 4, sum(number) as c, getSetting('max_threads') as mt from numbers_mt(100000000) group by mt;

select n, a, s != '1', s = toString(getSetting('max_threads')) from t order by n;

drop table t;