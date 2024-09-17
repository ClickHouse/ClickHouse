drop table if exists tab;
create table tab (x UInt32, y UInt32) engine = MergeTree order by x;

insert into tab select number, number from numbers(10);
insert into tab select number + 10, number + 10 from numbers(10);

set optimize_sorting_by_input_stream_properties=1;
set optimize_aggregation_in_order=1;
set enable_memory_bound_merging_of_aggregation_results=1;
set prefer_localhost_replica=1;

select sum(y) as s from remote('127.0.0.{1,2}', currentDatabase(), tab) group by x order by x;
select 'sort properties are reused after MergingAggregated';
select replaceAll(trimLeft(explain), '__table1.', '') from (explain actions = 1, sorting=1, description=0 select sum(y) as s from remote('127.0.0.{1,2}', currentDatabase(), tab) group by x order by x) where explain ilike '%sort%' or explain like '%ReadFromMergeTree%' or explain like '%Aggregat%';

select sum(y) as s from remote('127.0.0.{1,2}', currentDatabase(), tab) group by x order by x desc;
select 'choosing of sort order is not working';
select replaceAll(trimLeft(explain), '__table1.', '') from (explain actions = 1, sorting=1, description=0 select sum(y) as s from remote('127.0.0.{1,2}', currentDatabase(), tab) group by x order by x desc ) where explain ilike '%sort%' or explain like '%ReadFromMergeTree%' or explain like '%Aggregat%';

select sum(y) as s from remote('127.0.0.{1,2}', currentDatabase(), tab) group by x order by x, s;
select 'finish sorting is used';
select replaceAll(trimLeft(explain), '__table1.', '') from (explain actions = 1, sorting=1, description=0 select sum(y) as s from remote('127.0.0.{1,2}', currentDatabase(), tab) group by x order by x, s) where explain ilike '%sort%' or explain like '%ReadFromMergeTree%' or explain like '%Aggregat%';
