-- Regression test for the compact `EXPLAIN indexes=1` fast path: it must not
-- collapse a plan whose sources do not all report index statistics. Otherwise
-- a `UNION ALL` (or a `Merge` table) mixing a `MergeTree` table with a source
-- that has no indexes prints only the indexed branch and silently drops the
-- other one.

drop table if exists test_mixed_mt;
drop table if exists test_mixed_merge;

create table test_mixed_mt (key Int, value Int, index value_idx value type minmax granularity 1) engine=MergeTree() order by key settings index_granularity=1000;
insert into test_mixed_mt select number, number*100 from numbers(10000) settings max_block_size=100000, min_insert_block_size_rows=100000, max_insert_threads=1;

create table test_mixed_merge (key Int, value Int) engine=Merge(currentDatabase(), '^test_mixed_mt$');

set use_query_condition_cache=0;
set use_statistics_for_part_pruning=0;
set allow_experimental_parallel_reading_from_replicas=0;
set enable_analyzer=1;
set use_skip_indexes_on_data_read=0;
set secondary_indices_enable_bulk_filtering=0;
set query_plan_optimize_prewhere=1;
set optimize_move_to_prewhere=1;
set query_plan_remove_unused_columns=1;

-- Mixed `UNION ALL`: the `numbers` branch reports no indexes, so the plan must
-- be printed in full instead of being collapsed to the `MergeTree` summary.
-- { echo }
explain indexes=1, compact=1 select key from test_mixed_mt where value > 500000 union all select number::Int from numbers(10);
explain indexes=1, compact=1, json=1 select key from test_mixed_mt where value > 500000 union all select number::Int from numbers(10) format TSVRaw;

-- Homogeneous `UNION ALL` and a single-table `Merge`: the fast path still applies.
explain indexes=1, compact=1 select key from test_mixed_mt where value > 500000 union all select key from test_mixed_mt where value > 900000;
explain indexes=1, compact=1 select key from test_mixed_merge where value > 500000;
-- { echoOff }

drop table test_mixed_merge;
drop table test_mixed_mt;
