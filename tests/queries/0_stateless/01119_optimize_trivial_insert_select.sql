drop table if exists t;
create table t(n int, a Int64, s String) engine = MergeTree() order by a;

set enable_positional_arguments=0;
set optimize_trivial_insert_select=1;

-- due to aggregate functions, optimize_trivial_insert_select will not be applied
insert into t select 1, sum(number) as c, getSetting('max_threads') from numbers_mt(100000000) settings max_insert_threads=4, max_threads=2;
-- due to GROUP BY, optimize_trivial_insert_select will not be applied
insert into t select 2, sum(number) as c, getSetting('max_threads') from numbers_mt(100000000) group by 1 settings max_insert_threads=4, max_threads=2;
insert into t select 3, sum(number) as c, getSetting('max_threads') from numbers_mt(10000000) group by 3 settings max_insert_threads=4, max_threads=2;
insert into t select 4, sum(number) as c, getSetting('max_threads') as mt from numbers_mt(10000000) group by mt settings max_insert_threads=4, max_threads=2;

select n, a, s from t order by n;

drop table t;
