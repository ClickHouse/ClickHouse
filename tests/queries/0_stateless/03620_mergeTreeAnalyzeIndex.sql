-- Tags: no-random-merge-tree-settings
-- - no-random-merge-tree-settings -- may change amount of granulas

drop table if exists data;
create table data (key Int, value Int) engine=MergeTree() order by key;
insert into data select *, *+1000000 from numbers(100000);

-- { echo }
select * from mergeTreeAnalyzeIndex(currentDatabase(), data);
select * from mergeTreeAnalyzeIndex(currentDatabase(), data, '');
select * from mergeTreeAnalyzeIndex(currentDatabase(), data, '', key = 8193);
select * from mergeTreeAnalyzeIndex(currentDatabase(), data, '', key >= 8193);

select * from mergeTreeAnalyzeIndex(currentDatabase(), data, 'all_1_1_0', key = 8193);
select * from mergeTreeAnalyzeIndex(currentDatabase(), data, 'no_such_part', key = 8193);

-- Columns not from PK is allowed and ignored.
select * from mergeTreeAnalyzeIndex(currentDatabase(), data, '', value = 0);
select * from mergeTreeAnalyzeIndex(currentDatabase(), data, '', key = 8193 and value = 0);
