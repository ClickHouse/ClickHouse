drop table if exists test_10m;
create table test_10m (key Int, value Int) engine=MergeTree() order by key settings distributed_index_analysis_min_parts_to_activate=0, distributed_index_analysis_min_indexes_bytes_to_activate=0;
insert into test_10m select number, number*100 from numbers(1e6);

set allow_experimental_parallel_reading_from_replicas=0;
set cluster_for_parallel_replicas='parallel_replicas';

--- Ignore warnings when replica does not respond, and analysis is done on initiator
set send_logs_level='error';

select sum(key) from test_10m settings distributed_index_analysis=1, distributed_index_analysis_for_non_shared_merge_tree=0 format Null;
select sum(key) from test_10m settings distributed_index_analysis=1, distributed_index_analysis_for_non_shared_merge_tree=1 format Null;

system flush logs query_log;
select format(
  'distributed_index_analysis_for_non_shared_merge_tree={}, DistributedIndexAnalysisMicroseconds>0={}',
  Settings['distributed_index_analysis_for_non_shared_merge_tree'],
  ProfileEvents['DistributedIndexAnalysisMicroseconds'] > 0
)
from system.query_log
where
  current_database = currentDatabase()
  and event_date >= yesterday()
  and type = 'QueryFinish'
  and query_kind = 'Select'
  and is_initial_query
  and has(Settings, 'distributed_index_analysis')
  and endsWith(log_comment, '-' || currentDatabase())
order by event_time_microseconds;
