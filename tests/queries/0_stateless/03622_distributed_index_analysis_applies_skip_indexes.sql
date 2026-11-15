-- Tags: long

drop table if exists test_1m;
-- -min_bytes_for_wide_part -- wide parts are different (they respect index_granularity completely, unlike compact parts) -- FIXME
-- -index_granularity* -- test relies on number of granulas
create table test_1m (key Int, value Int, index value_idx value type minmax granularity 1) engine=MergeTree() order by key settings index_granularity=8192, min_bytes_for_wide_part=1e9, index_granularity_bytes=10e6, distributed_index_analysis_min_parts_to_activate=0, distributed_index_analysis_min_indexes_bytes_to_activate=0;
system stop merges test_1m;
insert into test_1m select number, number*100 from numbers(1e6) settings max_block_size=10000, min_insert_block_size_rows=10000, max_insert_threads=1;
select count(), sum(marks) from system.parts where database = currentDatabase() and table = 'test_1m' and active;

set cluster_for_parallel_replicas='test_cluster_one_shard_two_replicas';
set max_parallel_replicas=2;
set use_query_condition_cache=0;
-- Parallel replicas changes EXPLAIN output
set allow_experimental_parallel_reading_from_replicas=0;
set allow_experimental_analyzer=1;

-- { echo }
explain indexes=1 select * from test_1m where value > 800_000*100 settings distributed_index_analysis=0;
explain indexes=1 select * from test_1m where value > 800_000*100 settings distributed_index_analysis=1;
