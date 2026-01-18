-- Tags: no-random-merge-tree-settings
-- - no-random-merge-tree-settings -- may change amount of granulas
-- add_minmax_index_for_numeric_columns=0: Changes the plan (the index on value would filter out value = 0)

drop table if exists data;
create table data (key Int, value Int) engine=MergeTree() order by key settings add_minmax_index_for_numeric_columns=0;
insert into data select *, *+1000000 from numbers(100000);

-- { echo }
select * from mergeTreeAnalyzeIndexes(currentDatabase(), data);
select * from mergeTreeAnalyzeIndexes(currentDatabase(), data);
select * from mergeTreeAnalyzeIndexes(currentDatabase(), data, key = 8193);
select * from mergeTreeAnalyzeIndexes(currentDatabase(), data, key >= 8193);
select * from mergeTreeAnalyzeIndexes(currentDatabase(), data, key = 8192+1 or key = 8192*3+1);
select * from mergeTreeAnalyzeIndexes(currentDatabase(), data, key = 8192+1 or key = 8192*5+1);

select * from mergeTreeAnalyzeIndexes(currentDatabase(), data, key = 8193, 'all_1_1_0');
select * from mergeTreeAnalyzeIndexes(currentDatabase(), data, key = 8193, 'no_such_part');

-- Columns not from PK is allowed and ignored.
select * from mergeTreeAnalyzeIndexes(currentDatabase(), data, value = 0);
select * from mergeTreeAnalyzeIndexes(currentDatabase(), data, key = 8193 and value = 0);

-- Set
select * from mergeTreeAnalyzeIndexes(currentDatabase(), data, key in (8193, 16385));
