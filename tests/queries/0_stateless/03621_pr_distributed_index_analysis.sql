-- Tags: no-random-merge-tree-settings, no-random-settings
-- - no-random-merge-tree-settings -- may change number of parts

drop table if exists test_10m;
create table test_10m (key Int, value Int) engine=MergeTree() order by key;
system stop merges test_10m;
insert into test_10m select number, number*100 from numbers(10e6);

set parallel_replicas_for_non_replicated_merge_tree=1;
set parallel_replicas_index_analysis_only_on_coordinator=1;
set parallel_replicas_local_plan=1;

-- { echo }
select groupArraySortedDistinct(10)(_part), sum(key) from test_10m settings allow_experimental_parallel_reading_from_replicas=0;

select groupArraySortedDistinct(10)(_part), sum(key) from test_10m settings allow_experimental_parallel_reading_from_replicas=1, cluster_for_parallel_replicas='parallel_replicas', parallel_replicas_distributed_index_analysis=0;
select groupArraySortedDistinct(10)(_part), sum(key) from test_10m settings allow_experimental_parallel_reading_from_replicas=1, cluster_for_parallel_replicas='parallel_replicas', parallel_replicas_distributed_index_analysis=1;

select groupArraySortedDistinct(10)(_part), sum(key) from test_10m settings allow_experimental_parallel_reading_from_replicas=1, cluster_for_parallel_replicas='test_cluster_one_shard_two_replicas', parallel_replicas_distributed_index_analysis=0;
select groupArraySortedDistinct(10)(_part), sum(key) from test_10m settings allow_experimental_parallel_reading_from_replicas=1, cluster_for_parallel_replicas='test_cluster_one_shard_two_replicas', parallel_replicas_distributed_index_analysis=1;

select groupArraySortedDistinct(10)(_part), sum(key) from test_10m where key = 1 settings allow_experimental_parallel_reading_from_replicas=1, cluster_for_parallel_replicas='parallel_replicas', parallel_replicas_distributed_index_analysis=1;
select groupArraySortedDistinct(10)(_part), sum(key) from test_10m where key = 1 and value = 100 settings allow_experimental_parallel_reading_from_replicas=1, cluster_for_parallel_replicas='parallel_replicas', parallel_replicas_distributed_index_analysis=1;
select groupArraySortedDistinct(10)(_part), sum(key) from test_10m where key = 1 and value = 1 settings allow_experimental_parallel_reading_from_replicas=1, cluster_for_parallel_replicas='parallel_replicas', parallel_replicas_distributed_index_analysis=1;

-- { echoOff }
system flush logs query_log;
select format(
  'allow_experimental_parallel_reading_from_replicas={}, cluster_for_parallel_replicas={}, parallel_replicas_distributed_index_analysis={}, DistributedIndexAnalysisMicroseconds>0={}, DistributedIndexAnalysisScheduledReplicas={}, DistributedIndexAnalysisFailedReplicas>0={}',
  Settings['allow_experimental_parallel_reading_from_replicas'],
  Settings['cluster_for_parallel_replicas'],
  Settings['parallel_replicas_distributed_index_analysis'],
  ProfileEvents['DistributedIndexAnalysisMicroseconds'] > 0,
  ProfileEvents['DistributedIndexAnalysisScheduledReplicas'],
  ProfileEvents['DistributedIndexAnalysisFailedReplicas'] > 0
)
from system.query_log
where
  current_database = currentDatabase()
  and event_date >= yesterday()
  and type != 'QueryStart'
  and is_initial_query
  and has(Settings, 'allow_experimental_parallel_reading_from_replicas')
  and endsWith(log_comment, '-' || currentDatabase())
order by event_time_microseconds;
