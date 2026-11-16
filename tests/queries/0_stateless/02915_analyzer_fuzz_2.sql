SET aggregate_functions_null_for_empty = 1;
--set enable_analyzer=1;
create table t_delete_projection (x UInt32, y UInt64, projection p (select sum(y))) engine = MergeTree order by tuple() SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
insert into t_delete_projection select number, toString(number) from numbers(8192 * 10);
