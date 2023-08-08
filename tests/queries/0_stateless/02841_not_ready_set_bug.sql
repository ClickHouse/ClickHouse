drop table if exists t1;
create table t1 (number UInt64) engine = MergeTree order by tuple();
insert into t1 select number from numbers(10);
set max_threads=2, max_result_rows = 1, result_overflow_mode = 'break';
with tab as (select min(number) from t1 prewhere number in (select number from view(select number, row_number() OVER (partition by number % 2 ORDER BY number DESC) from numbers_mt(1e4)) where number != 2 order by number)) select number from t1 union all select * from tab;
