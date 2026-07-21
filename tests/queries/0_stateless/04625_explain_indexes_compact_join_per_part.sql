-- Regression tests for the compact `EXPLAIN indexes=1` fast path:
-- 1. It must not flat-aggregate index stats across `JOIN` inputs (only `Merge`
--    children, distributed replicas and `UNION` branches are supported shapes).
-- 2. It must not aggregate per-part stats (`per_part_index_stats=1`), where the
--    same index type appears once per part and "keep the last entry per type"
--    would undercount.

drop table if exists test_compact_a;
drop table if exists test_compact_b;

create table test_compact_a (key Int, value Int, index value_idx value type minmax granularity 1) engine=MergeTree() order by key settings index_granularity=1000;
system stop merges test_compact_a;
insert into test_compact_a select number, number*100 from numbers(10000) settings max_block_size=100000, min_insert_block_size_rows=100000, max_insert_threads=1;
insert into test_compact_a select number+10000, (number+10000)*100 from numbers(10000) settings max_block_size=100000, min_insert_block_size_rows=100000, max_insert_threads=1;
select count() from system.parts where database = currentDatabase() and table = 'test_compact_a' and active;

create table test_compact_b (key Int, value Int) engine=MergeTree() order by key settings index_granularity=1000;
system stop merges test_compact_b;
insert into test_compact_b select number, number*10 from numbers(10000) settings max_block_size=100000, min_insert_block_size_rows=100000, max_insert_threads=1;

set use_query_condition_cache=0;
set use_statistics_for_part_pruning=0;
set allow_experimental_parallel_reading_from_replicas=0;
set enable_analyzer=1;
set query_plan_join_swap_table=0;
set join_algorithm='hash';
set enable_join_runtime_filters=0;
set use_hash_table_stats_for_join_reordering=0;
set use_statistics=0;
set query_plan_optimize_join_order_limit=0;
set use_skip_indexes_on_data_read=0;
set secondary_indices_enable_bulk_filtering=0;
set query_plan_optimize_prewhere=1;
set optimize_move_to_prewhere=1;

-- JOIN: both sides keep their own index stats (no flat aggregation under the join header).
-- { echo }
explain indexes=1, compact=1 select * from test_compact_a a inner join test_compact_b b on a.key = b.key where a.key > 15000 and b.key > 5000;
-- { echoOff }

-- Per-part stats: each part keeps its own skip-index row (no "last entry per type" collapse).
set per_part_index_stats=1;
-- { echo }
explain indexes=1, compact=1 select * from test_compact_a where value > 1500000;
explain indexes=1, compact=1 select key from test_compact_a where value > 1500000 union all select key from test_compact_a where value <= 500000;
-- { echoOff }
set per_part_index_stats=0;

-- Sanity check: without per-part stats the UNION fast path still aggregates.
-- { echo }
explain indexes=1, compact=1 select key from test_compact_a where value > 1500000 union all select key from test_compact_a where value <= 500000;
-- { echoOff }

drop table test_compact_a;
drop table test_compact_b;
