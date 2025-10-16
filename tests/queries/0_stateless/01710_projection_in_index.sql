SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0.0;

drop table if exists t;

create table t (i int, j int, k int, projection p (select * order by j)) engine MergeTree order by i settings index_granularity = 1;

insert into t select number, number, number from numbers(10);

set optimize_use_projections = 1, max_rows_to_read = 3;

select * from t where i < 5 and j in (1, 2);

drop table t;

drop table if exists test;

create table test (name String, time Int64) engine MergeTree order by time;

insert into test values ('hello world', 1662336000241);

select count() from (select fromUnixTimestamp64Milli(time, 'UTC') time_fmt, name from test where time_fmt > '2022-09-05 00:00:00');

drop table test;
