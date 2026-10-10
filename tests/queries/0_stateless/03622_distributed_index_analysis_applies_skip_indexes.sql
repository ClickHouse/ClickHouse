-- Tags: long
SET explain_query_plan_default = 'legacy';

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
-- Disable statistics-based part pruning so that randomized auto_statistics_types
-- (which may include 'minmax') does not add a Statistics step to EXPLAIN output.
set use_statistics_for_part_pruning=0;
-- Parallel replicas changes EXPLAIN output
set allow_experimental_parallel_reading_from_replicas=0;
set allow_experimental_analyzer=1;
set query_plan_optimize_prewhere=1;
set optimize_move_to_prewhere=1;
-- Ignore `Cannot connect to {}. It will not participate in distributed index analysis`
set send_logs_level='error';

-- { echo }
explain indexes=1 select * from test_1m where value > 800_000*100 settings distributed_index_analysis=0;
-- The `Distributed:` block lists one row per replica of `test_cluster_one_shard_two_replicas`.
-- 127.0.0.2 is not guaranteed to be up, and when it is down its row degenerates to an empty
-- `Address:` with zero counters while the local replica absorbs its parts. Assert the
-- aggregate `Parts:`/`Granules:` instead: under `distributed_index_analysis=1` those come from
-- the ranges the replicas returned, so they still prove the skip index was applied remotely.
select * from (
    explain indexes=1 select * from test_1m where value > 800_000*100
    settings distributed_index_analysis=1
) where explain not like '%Address:%'
    and explain not like '%Parts send:%' and explain not like '%Parts received:%'
    and explain not like '%Granules send:%' and explain not like '%Granules received:%';

-- { echoOff }
-- The filtered plan above no longer names the replica that answered, so assert separately that the
-- remote replica analyzed its own parts rather than silently falling back to the initiator. Both
-- `DistributedIndexAnalysisReplicaFallback` and `DistributedIndexAnalysisMissingParts` are
-- incremented only once a replica has connected and its analysis then failed, so they read 0
-- whether or not 127.0.0.2 is up. `DistributedIndexAnalysisScheduledReplicas` and
-- `DistributedIndexAnalysisReplicaUnavailable` are deliberately not asserted: they depend on
-- replica availability and would reintroduce the flakiness this test avoids.
system flush logs query_log;
select format(
  'DistributedIndexAnalysisMicroseconds>0={}, DistributedIndexAnalysisReplicaFallback={}, DistributedIndexAnalysisMissingParts={}',
  ProfileEvents['DistributedIndexAnalysisMicroseconds'] > 0,
  ProfileEvents['DistributedIndexAnalysisReplicaFallback'],
  ProfileEvents['DistributedIndexAnalysisMissingParts']
)
from system.query_log
where
  current_database = currentDatabase()
  and event_date >= yesterday() and event_time >= now() - 600
  and type = 'QueryFinish'
  and is_initial_query
  and endsWith(log_comment, '-' || currentDatabase())
  -- The statement above is a `select` over a subquery, not an `explain`, so its kind is `Select`.
  and query_kind = 'Select'
  and position(query, 'explain indexes=1') > 0
  -- This observing statement also contains that literal, so exclude it by the fact that it is the
  -- only statement here reading `system`.
  and not has(databases, 'system')
-- Newest match only, so a rerun in a fixed database cannot add a second line.
order by event_time_microseconds desc
limit 1;
