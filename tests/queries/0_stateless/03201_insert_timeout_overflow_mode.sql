drop table if exists a sync;
create table a (x Int8) engine ReplicatedMergeTree('/tables/{database}/a','0') order by x;
insert into a select sleepEachRow(1) from numbers(10000) settings max_block_size=1, min_insert_block_size_rows=1, max_execution_time=2, timeout_overflow_mode='break';
insert into a select sleepEachRow(1) from numbers(10000) settings max_block_size=1, min_insert_block_size_rows=1, max_execution_time=2, timeout_overflow_mode='break', deduplicate_blocks_in_dependent_materialized_views=1;
select count() < 30 from a;
