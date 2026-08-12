-- Tags: no-random-merge-tree-settings
-- - no-random-merge-tree-settings -- may change amount of granulas

drop table if exists with_skip_index;
create table with_skip_index (key Int, value Int, index value_idx value type minmax granularity 1) engine=MergeTree() order by key;
insert into with_skip_index select number, number*100 from numbers(1e6);

-- { echo }
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), 'with_skip_index', value > 0);
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), 'with_skip_index', value > 5_000_000);
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), 'with_skip_index', value > 100_000_000);
