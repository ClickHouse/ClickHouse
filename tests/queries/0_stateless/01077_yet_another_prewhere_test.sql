drop table if exists t50;

create table t50 (a Int, b Int, s String) engine = MergeTree order by a settings index_granularity = 50, index_granularity_bytes=1000, min_index_granularity_bytes=500;

-- some magic to satisfy conditions to run optimizations in MergeTreeRangeReader
insert into t50 select 0, 1, repeat('a', 10000);
insert into t50 select number, multiIf(number < 5, 1, number < 50, 0, number < 55, 1, number < 100, 0, number < 105, 1, 0), '' from numbers(150);
optimize table t50 final;

select a, b from t50 prewhere b = 1 order by a;

drop table t50;
