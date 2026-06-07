-- Tags: long

SET max_rows_to_read = '100M', max_execution_time = 600;
drop table if exists lc_00906;
create table lc_00906 (b LowCardinality(String)) engine=MergeTree order by b SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi', vertical_merge_algorithm_min_rows_to_activate=100000000;
insert into lc_00906 select '0123456789' from numbers(100000000) SETTINGS max_insert_threads=6, max_threads=4;
-- Aggregating a single `LowCardinality` group produces one row, so a single thread is enough.
-- Pinning `max_threads = 1` avoids per-thread aggregation-state allocations that, combined with
-- randomized parallel replicas, can push the runner over its memory budget under shared load.
select count(), b from lc_00906 group by b SETTINGS max_threads = 1, enable_parallel_replicas = 0;
drop table if exists lc_00906;
