-- Tags: long, no-parallel

-- Generate many parts (partitions) to ensure that all replicas will be chosen for distributed index analysis
-- even failed replica (that is included into parallel_replicas cluster), and ensure that the SELECT wont fail (parts should be analyzed locally).

drop table if exists test_10m;
create table test_10m (key Int, value Int) engine=MergeTree() order by key partition by key % 200 settings distributed_index_analysis_min_parts_to_activate=0, distributed_index_analysis_min_indexes_bytes_to_activate=0;
system stop merges test_10m;

insert into test_10m select number, number*100 from numbers(1e6) settings max_partitions_per_insert_block=200, max_block_size=1e6;

set allow_experimental_parallel_reading_from_replicas=0;
set parallel_replicas_for_non_replicated_merge_tree=1;
set parallel_replicas_index_analysis_only_on_coordinator=1;
set parallel_replicas_local_plan=1;
set distributed_index_analysis=1;
set max_parallel_replicas=100;
set cluster_for_parallel_replicas='parallel_replicas';
--- Ignore warnings when replica does not respond, and analysis is done on initiator
set send_logs_level='error';

-- { echo }
select sum(key) from test_10m;
select sum(key) from test_10m where key = 1;

-- { echoOff }
system flush logs query_log;
select
  normalizeQuery(query) q,
  ProfileEvents['DistributedIndexAnalysisMicroseconds'] > 0 not_blazingly_fast,
  ProfileEvents['DistributedIndexAnalysisMissingParts'] missing_parts,
  ProfileEvents['DistributedIndexAnalysisScheduledReplicas'] replicas,
  ProfileEvents['DistributedIndexAnalysisFailedReplicas'] > 0 failed_replicas
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
