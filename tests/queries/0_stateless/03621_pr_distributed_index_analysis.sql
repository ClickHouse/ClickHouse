-- Make sure that distributed index analysis works with parallel replicas

drop table if exists test_10m;
create table test_10m (key Int, value Int) engine=MergeTree() order by key;
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
  normalizeQuery(query) q,
  ProfileEvents['DistributedIndexAnalysisScheduledReplicas'] > 0 distributed_index_analysis_replicas,
  ProfileEvents['ParallelReplicasReadRequestMicroseconds'] > 0 read_with_parallel_replicas
from system.query_log
where
  current_database = currentDatabase()
  and event_date >= yesterday()
  and type != 'QueryStart'
  and query_kind = 'Select'
  and is_initial_query
  and Settings['distributed_index_analysis'] = '1'
  and endsWith(log_comment, '-' || currentDatabase())
order by event_time_microseconds
format Vertical;
