-- Tags: no-replicated-database, long
-- - no-replicated-database - it uses cluster of multiple nodes, while we use different clusters

-- Make sure that distributed index analysis works with parallel replicas

drop table if exists test_10m;
create table test_10m (key Int, value Int) engine=MergeTree() order by key settings distributed_index_analysis_min_parts_to_activate=0, distributed_index_analysis_min_indexes_bytes_to_activate=0;
system stop merges test_10m;
insert into test_10m select number, number*100 from numbers(10e6);

set parallel_replicas_for_non_replicated_merge_tree=1;
set parallel_replicas_index_analysis_only_on_coordinator=1;
set parallel_replicas_local_plan=1;
--- Ignore warnings when replica does not respond, and analysis is done on initiator
set send_logs_level='error';

-- { echo }
select sum(key) from test_10m settings allow_experimental_parallel_reading_from_replicas=0, distributed_index_analysis=1, cluster_for_parallel_replicas='parallel_replicas';
select sum(key) from test_10m settings allow_experimental_parallel_reading_from_replicas=1, distributed_index_analysis=1, cluster_for_parallel_replicas='parallel_replicas';
select sum(key) from test_10m settings allow_experimental_parallel_reading_from_replicas=1, distributed_index_analysis=1, cluster_for_parallel_replicas='test_cluster_one_shard_two_replicas';

-- { echoOff }
system flush logs query_log;
select
  anyIf(normalizeQuery(query), is_initial_query) q,
  if(
    -- in case of parallel replicas, it is possible that there will be less subqueries then max, but should not exceed it
    any(Settings['allow_experimental_parallel_reading_from_replicas']) = '1',
    if(any(Settings['cluster_for_parallel_replicas']) = 'parallel_replicas',
      -- the max is hosts in parallel_replicas (10) * 2 (one for the query itself and one for mergeTreeAnalyzeIndexes()) - 1 * 2 (for local replica we do not issue a separate query) + 1 (query itself)
      max2(count(), 19)::UInt64,
      -- the max is hosts in test_cluster_one_shard_two_replicas (2) * 2 (one for the query itself and one for mergeTreeAnalyzeIndexes()) - 1 * 2 (for local replica we do not issue a separate query) + 1 (query itself)
      max2(count(), 3)::UInt64,
    ),
    -- in case of distributed index analyss, it is possible that there will be less subqueries then max, due to failures on remote
    if(any(Settings['cluster_for_parallel_replicas']) = 'parallel_replicas',
      max2(count(), 10)::UInt64,
      max2(count(), 2)::UInt64
    )
  ) queries_with_subqueries,
  anyIf(ProfileEvents['DistributedIndexAnalysisScheduledReplicas'] > 0, is_initial_query) distributed_index_analysis_replicas,
  anyIf(ProfileEvents['ParallelReplicasUsedCount'] > 0, is_initial_query) read_with_parallel_replicas
from system.query_log
where
  event_date >= yesterday()
  and type = 'QueryFinish'
  and query_kind = 'Select'
  and Settings['distributed_index_analysis'] = '1'
  -- SKIP: current_database = currentDatabase() (it will filter out non-initial queries)
  and endsWith(log_comment, '-' || currentDatabase())
group by initial_query_id
order by min(event_time_microseconds)
format Vertical;
