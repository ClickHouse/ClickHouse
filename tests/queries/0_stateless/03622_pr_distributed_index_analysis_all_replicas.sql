-- Tags: no-random-merge-tree-settings, no-random-settings
-- - no-random-merge-tree-settings -- may change number of parts

-- Generate many parts (partitions) to ensure that all replicas will be chosen for distributed index analysis
-- even failed replica (that is included into parallel_replcas), and ensure that the SELECT wont fail (parts should be analyzed locally).

drop table if exists test_10m;
create table test_10m (key Int, value Int) engine=MergeTree() order by key partition by key % 100;
system stop merges test_10m;

insert into test_10m select number, number*100 from numbers(1e6) settings max_partitions_per_insert_block=100, max_block_size=1e6;

set parallel_replicas_for_non_replicated_merge_tree=1;
set parallel_replicas_index_analysis_only_on_coordinator=1;
set parallel_replicas_local_plan=1;
set distributed_index_analysis=1;
set cluster_for_parallel_replicas='parallel_replicas';
--- Ignore warnings when replica does not respond, and analysis is done on initiator
set send_logs_level='error';

-- { echo }
select sum(key) from test_10m settings allow_experimental_parallel_reading_from_replicas=1;
select sum(key) from test_10m where key = 1 settings allow_experimental_parallel_reading_from_replicas=1;

-- { echoOff }
system flush logs query_log;
select format(
  'allow_experimental_parallel_reading_from_replicas={}, cluster_for_parallel_replicas={}, distributed_index_analysis={}, DistributedIndexAnalysisMicroseconds>0={}, DistributedIndexAnalysisMissingParts={}, DistributedIndexAnalysisScheduledReplicas={}, DistributedIndexAnalysisFailedReplicas>0={}',
  Settings['allow_experimental_parallel_reading_from_replicas'],
  Settings['cluster_for_parallel_replicas'],
  Settings['distributed_index_analysis'],
  ProfileEvents['DistributedIndexAnalysisMicroseconds'] > 0,
  ProfileEvents['DistributedIndexAnalysisMissingParts'],
  ProfileEvents['DistributedIndexAnalysisScheduledReplicas'],
  ProfileEvents['DistributedIndexAnalysisFailedReplicas'] > 0
)
from system.query_log
where
  current_database = currentDatabase()
  and event_date >= yesterday()
  and type != 'QueryStart'
  and query_kind = 'Select'
  and is_initial_query
  and has(Settings, 'allow_experimental_parallel_reading_from_replicas')
  and endsWith(log_comment, '-' || currentDatabase())
order by event_time_microseconds;
