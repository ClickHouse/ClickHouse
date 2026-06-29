drop table if exists data;
create table data (key Int, value Int) engine=MergeTree() order by key settings merge_selector_base = 1000, index_granularity=8192, index_granularity_bytes=10e9, min_bytes_for_wide_part=1e9 as select *, *+1000000 from numbers(100000) settings max_insert_threads=1;

drop table if exists keys_1;
create table keys_1 (key Int) engine=Log() as select * from numbers(10);

drop table if exists keys_2;
create table keys_2 (key Int) engine=Log() as select * from numbers(10000, 10);

-- { echo }
select * from mergeTreeAnalyzeIndexes(currentDatabase(), data, key in (select key from data order by key limit 10));
select * from mergeTreeAnalyzeIndexes(currentDatabase(), data, key in (select key from data order by key desc limit 10));
select * from mergeTreeAnalyzeIndexes(currentDatabase(), data, key in keys_1);
select * from mergeTreeAnalyzeIndexes(currentDatabase(), data, key in keys_2);
