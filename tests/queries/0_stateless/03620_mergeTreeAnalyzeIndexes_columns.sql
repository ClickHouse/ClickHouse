-- Tags: no-random-merge-tree-settings
-- - no-random-merge-tree-settings -- may change amount of granulas

drop table if exists data;
create table data (key Int, value Int) engine=MergeTree() order by key;
system stop merges data;
insert into data select *, *+1000000 from numbers(100000);
insert into data select *, *+1000000 from numbers(100000, 200000);

-- { echo }
select part_name from mergeTreeAnalyzeIndexes(currentDatabase(), data, key >= 1000);
select ranges from mergeTreeAnalyzeIndexes(currentDatabase(), data, key >= 1000);
select arraySum(arrayMap(e -> ((e.2) - (e.1)), ranges)) as ranges_size from mergeTreeAnalyzeIndexes(currentDatabase(), data, key >= 1000);
select sum(arraySum(arrayMap(e -> ((e.2) - (e.1)), ranges))) as ranges_size from mergeTreeAnalyzeIndexes(currentDatabase(), data, key >= 1000);
