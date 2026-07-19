-- Answering filterless min/max aggregation from per-part column statistics
-- (an extension of the implicit minmax_count projection).

set explain_query_plan_default = 'legacy';
set materialize_statistics_on_insert = 1;
set optimize_use_projections = 1, optimize_use_implicit_projections = 1;
set use_statistics_for_min_max_aggregation = 1;
set parallel_replicas_local_plan = 1, parallel_replicas_support_projection = 1;
set optimize_aggregation_in_order = 0;
set mutations_sync = 2, lightweight_deletes_sync = 2;

drop table if exists t_stats_minmax;

create table t_stats_minmax (key UInt64, date Date, value Int32, n Nullable(Int32), s String)
engine = MergeTree order by (key, date)
settings auto_statistics_types = 'basic';

insert into t_stats_minmax select number, toDate('2024-01-01') + number % 365, toInt32(number % 1000) - 500, number, toString(number) from numbers(10000);
insert into t_stats_minmax select number + 10000, toDate('2025-01-01') + number % 365, toInt32(number % 2000) - 1000, number, toString(number) from numbers(10000);

select '-- answered from per-part statistics, no column data is read';
set max_rows_to_read = 4;
select min(date), max(date) from t_stats_minmax;
select min(date), max(date), min(value), max(value), count() from t_stats_minmax;
select max(value), min(date) from t_stats_minmax;
-- min/max of the first primary key column come from the primary index, combined with statistics
select min(key), max(key), max(date) from t_stats_minmax;
set max_rows_to_read = 0;
select count() from (explain select min(date), max(date) from t_stats_minmax) where explain like '%_minmax_count_projection%';

select '-- same results with the optimization disabled';
select min(date), max(date), min(value), max(value), count() from t_stats_minmax settings use_statistics_for_min_max_aggregation = 0;
select count() from (explain select min(date), max(date) from t_stats_minmax settings use_statistics_for_min_max_aggregation = 0) where explain like '%_minmax_count_projection%';

select '-- Nullable and String fall back to a normal read';
select min(n), max(n) from t_stats_minmax;
select min(s), max(s) from t_stats_minmax;
select count() from (explain select min(n), max(n) from t_stats_minmax) where explain like '%_minmax_count_projection%';
select count() from (explain select min(s), max(s) from t_stats_minmax) where explain like '%_minmax_count_projection%';

select '-- min/max of a monotonic function of an eligible column is also optimized';
select min(date + 1), max(date + 1) from t_stats_minmax;
select count() from (explain select min(date + 1), max(date + 1) from t_stats_minmax) where explain like '%_minmax_count_projection%';

select '-- unmatched aggregate disables the whole candidate';
select min(date), sum(value) from t_stats_minmax;
select count() from (explain select min(date), sum(value) from t_stats_minmax) where explain like '%_minmax_count_projection%';

select '-- lightweight delete disables the optimization';
delete from t_stats_minmax where key < 5;
select min(key), min(date), max(date) from t_stats_minmax;
select count() from (explain select min(date), max(date) from t_stats_minmax) where explain like '%_minmax_count_projection%';

drop table t_stats_minmax;

select '-- mutation rebuilds statistics of the updated column';
drop table if exists t_stats_minmax_update;
create table t_stats_minmax_update (key UInt64, value Int32)
engine = MergeTree order by key
settings auto_statistics_types = 'basic';
insert into t_stats_minmax_update select number, toInt32(number) from numbers(1000);
insert into t_stats_minmax_update select number + 1000, toInt32(number + 1000) from numbers(1000);
alter table t_stats_minmax_update update value = 42 where 1;
set max_rows_to_read = 4;
select min(value), max(value) from t_stats_minmax_update;
set max_rows_to_read = 0;
select count() from (explain select min(value), max(value) from t_stats_minmax_update) where explain like '%_minmax_count_projection%';
drop table t_stats_minmax_update;

select '-- a part without statistics falls back to a normal read';
drop table if exists t_stats_minmax_mixed;
create table t_stats_minmax_mixed (key UInt64, value Int32)
engine = MergeTree order by key
settings auto_statistics_types = '';
insert into t_stats_minmax_mixed select number, toInt32(number) from numbers(1000);
alter table t_stats_minmax_mixed modify setting auto_statistics_types = 'basic';
insert into t_stats_minmax_mixed select number + 1000, toInt32(number + 1000) from numbers(1000);
select min(value), max(value) from t_stats_minmax_mixed;
select count() from (explain select min(value), max(value) from t_stats_minmax_mixed) where explain like '%_minmax_count_projection%';
drop table t_stats_minmax_mixed;

select '-- group by partition key combined with statistics-backed min/max';
drop table if exists t_stats_minmax_part;
create table t_stats_minmax_part (p UInt8, v UInt32)
engine = MergeTree partition by p order by tuple()
settings auto_statistics_types = 'basic';
insert into t_stats_minmax_part select 0, number from numbers(1000);
insert into t_stats_minmax_part select 1, number + 100000 from numbers(1000);
set max_rows_to_read = 4;
select p, min(v), max(v), count() from t_stats_minmax_part group by p order by p;
set max_rows_to_read = 0;
drop table t_stats_minmax_part;
