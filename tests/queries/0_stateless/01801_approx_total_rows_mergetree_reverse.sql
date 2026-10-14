drop table if exists data_01801;
create table data_01801 (key Int) engine=MergeTree() order by key settings index_granularity=10 as select number/10 from numbers(100);

-- Prevent remote replicas from skipping index analysis in Parallel Replicas. Otherwise, they may return full ranges and trigger max_rows_to_read validation failures.
SET parallel_replicas_index_analysis_only_on_coordinator = 0;

select * from data_01801 where key = 0 order by key settings max_rows_to_read=9 format Null; -- { serverError TOO_MANY_ROWS }
select * from data_01801 where key = 0 order by key desc settings max_rows_to_read=9 format Null; -- { serverError TOO_MANY_ROWS }

select * from data_01801 where key = 0 order by key settings max_rows_to_read=10 format Null;
select * from data_01801 where key = 0 order by key desc settings max_rows_to_read=10 format Null;

drop table data_01801;
