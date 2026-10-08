-- Tags: no-random-merge-tree-settings, no-random-settings, no-replicated-database
-- - no-random-merge-tree-settings -- may change number of parts
-- - no-replicated-database -- uses clusters with multiple nodes

-- Verify that all four combinations of use_hedged_requests x async_socket_for_remote
-- produce correct results and expected profile events for distributed index analysis.

drop table if exists test_dia_modes;
create table test_dia_modes (key Int, value Int) engine=MergeTree() order by key settings distributed_index_analysis_min_parts_to_activate=0, distributed_index_analysis_min_indexes_bytes_to_activate=0;
system stop merges test_dia_modes;
insert into test_dia_modes select number, number*100 from numbers(10e6);

set allow_experimental_parallel_reading_from_replicas=0;
set max_parallel_replicas=8;
--- Ignore warnings when replica does not respond, and analysis is done on initiator
set send_logs_level='error';

-- { echo }
select count() from system.parts where database = currentDatabase() and table = 'test_dia_modes';

select sum(key) from test_dia_modes settings cluster_for_parallel_replicas='parallel_replicas', distributed_index_analysis=1, use_hedged_requests=1, async_socket_for_remote=1;
select sum(key) from test_dia_modes settings cluster_for_parallel_replicas='parallel_replicas', distributed_index_analysis=1, use_hedged_requests=1, async_socket_for_remote=0;
select sum(key) from test_dia_modes settings cluster_for_parallel_replicas='parallel_replicas', distributed_index_analysis=1, use_hedged_requests=0, async_socket_for_remote=1;
select sum(key) from test_dia_modes settings cluster_for_parallel_replicas='parallel_replicas', distributed_index_analysis=1, use_hedged_requests=0, async_socket_for_remote=0;

select sum(key) from test_dia_modes settings cluster_for_parallel_replicas='parallel_replicas_unavailable_first', distributed_index_analysis=1, use_hedged_requests=1, async_socket_for_remote=1;
select sum(key) from test_dia_modes settings cluster_for_parallel_replicas='parallel_replicas_unavailable_first', distributed_index_analysis=1, use_hedged_requests=1, async_socket_for_remote=0;
select sum(key) from test_dia_modes settings cluster_for_parallel_replicas='parallel_replicas_unavailable_first', distributed_index_analysis=1, use_hedged_requests=0, async_socket_for_remote=1;
select sum(key) from test_dia_modes settings cluster_for_parallel_replicas='parallel_replicas_unavailable_first', distributed_index_analysis=1, use_hedged_requests=0, async_socket_for_remote=0;

-- { echoOff }
system flush logs query_log;
-- First run: check profile events (Connects is non-deterministic for first queries
-- because the pool may already have connections from earlier work).
select format(
  'cluster={}, hedged={}, async={}, Micros>0={}, Scheduled={}, Unavailable matches={}, Fallback={}, MissingParts={}',
  Settings['cluster_for_parallel_replicas'],
  Settings['use_hedged_requests'],
  Settings['async_socket_for_remote'],
  ProfileEvents['DistributedIndexAnalysisMicroseconds'] > 0,
  ProfileEvents['DistributedIndexAnalysisScheduledReplicas'],
  if (Settings['use_hedged_requests']::UInt64 == 1,
    ProfileEvents['DistributedIndexAnalysisReplicaUnavailable'] in (0, 1),
    ProfileEvents['DistributedIndexAnalysisReplicaUnavailable'] == 1
  ),
  ProfileEvents['DistributedIndexAnalysisReplicaFallback'],
  ProfileEvents['DistributedIndexAnalysisMissingParts']
)
from system.query_log
where
  current_database = currentDatabase()
  and event_date >= yesterday() AND event_time >= now() - 600
  and type = 'QueryFinish'
  and query_kind = 'Select'
  and is_initial_query
  and Settings['distributed_index_analysis'] = '1'
  and endsWith(log_comment, '-' || currentDatabase())
order by event_time_microseconds;

drop table test_dia_modes;
