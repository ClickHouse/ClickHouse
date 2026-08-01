-- Tags: long
SET explain_query_plan_default = 'legacy';

drop table if exists test_1m;
-- -min_bytes_for_wide_part -- wide parts are different (they respect index_granularity completely, unlike compact parts) -- FIXME
-- -merge_selector_base = 1000 -- disable merges
-- -index_granularity* -- test relies on number of granulas
create table test_1m (key Int, value Int) engine=MergeTree() order by key settings merge_selector_base = 1000, index_granularity=8192, min_bytes_for_wide_part=1e9, index_granularity_bytes=10e6, distributed_index_analysis_min_parts_to_activate=0, distributed_index_analysis_min_indexes_bytes_to_activate=0;
system stop merges test_1m;
insert into test_1m select number, number*100 from numbers(1e6) settings max_block_size=10000, min_insert_block_size_rows=10000, max_insert_threads=1;
select count(), sum(marks) from system.parts where database = currentDatabase() and table = 'test_1m' and active;

set cluster_for_parallel_replicas='test_cluster_one_shard_two_replicas';
set distributed_index_analysis=1;
set max_parallel_replicas=2;
set use_query_condition_cache=0;
set additional_table_filters={'test_1m': 'key > 10000'};
-- Only with analyzer
set allow_experimental_analyzer=1;
-- Parallel replicas changes EXPLAIN output
set allow_experimental_parallel_reading_from_replicas=0;
-- disable statistics-based part pruning to keep EXPLAIN output stable
SET use_statistics_for_part_pruning = 0;
-- Ignore "Cannot connect to {}. It will not participate in distributed index analysis"
set send_logs_level='error';

-- { echo }
select count() from (select * from test_1m);
-- The `Distributed:` block lists one row per replica of test_cluster_one_shard_two_replicas.
-- 127.0.0.2 is not guaranteed to be up, and when it is down its row degenerates to an empty
-- `Address:` with zero counters while the local replica absorbs its parts. Assert the
-- aggregate `Parts:`/`Granules:` and the pushed-down `Condition:` instead.
select * from (
    explain indexes=1 select key from (select * from test_1m)
) where explain not like '%Address:%'
    and explain not like '%Parts send:%' and explain not like '%Parts received:%'
    and explain not like '%Granules send:%' and explain not like '%Granules received:%';
