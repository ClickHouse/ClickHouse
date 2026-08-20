-- Tags: no-random-merge-tree-settings, no-random-settings
-- - no-random-merge-tree-settings -- may change number of parts

drop table if exists test_dia_proj;
create table test_dia_proj
(
    key Int,
    value Int,
    projection prj_value
    (
        select key, value order by value
    )
)
engine=MergeTree()
order by key
settings distributed_index_analysis_min_parts_to_activate=0, distributed_index_analysis_min_indexes_bytes_to_activate=0;

system stop merges test_dia_proj;
insert into test_dia_proj select number, number from numbers(0, 1000000);
insert into test_dia_proj select number, number from numbers(1000000, 1000000);
insert into test_dia_proj select number, number from numbers(2000000, 1000000);

set allow_experimental_analyzer=1;
set max_rows_to_read=0;
set allow_experimental_parallel_reading_from_replicas=0;
set cluster_for_parallel_replicas='';
set max_parallel_replicas=100;
set distributed_index_analysis_for_non_shared_merge_tree=1;
-- Ranges cached by the first (correct) run would narrow the analysis of the second run
set use_query_condition_cache=0;

-- The rows limit fits reading via the projection (one granule per matching part), but not a full scan
select count(), sum(key) from test_dia_proj where value in (500000, 2500000) settings max_rows_to_read=1000000, optimize_use_projections=0; -- { serverError TOO_MANY_ROWS }

--- Ignore warnings when replica does not respond, and analysis is done on initiator
set send_logs_level='error';

-- { echo }
-- The filter matches all rows in all parts (the projection is analyzed, but rejected as not better)
select count(), sum(key) from test_dia_proj where value >= 0 settings distributed_index_analysis=0;
select count(), sum(key) from test_dia_proj where value >= 0 settings cluster_for_parallel_replicas='parallel_replicas', distributed_index_analysis=1;

-- The filter matches one row in the first part and one row in the last part (the projection is selected for reading)
select count(), sum(key) from test_dia_proj where value in (500000, 2500000) settings distributed_index_analysis=0;
select count(), sum(key) from test_dia_proj where value in (500000, 2500000) settings cluster_for_parallel_replicas='parallel_replicas', distributed_index_analysis=1;

-- The rows limit fits reading via the projection, but not a full scan (see the failing query above)
select count(), sum(key) from test_dia_proj where value in (500000, 2500000) settings cluster_for_parallel_replicas='parallel_replicas', distributed_index_analysis=1, max_rows_to_read=1000000;

-- { echoOff }
system flush logs query_log;
select format(
  'distributed_index_analysis={}, DistributedIndexAnalysisMicroseconds>0={}, DistributedIndexAnalysisMissingParts={}, DistributedIndexAnalysisScheduledReplicas>0={}',
  Settings['distributed_index_analysis'],
  ProfileEvents['DistributedIndexAnalysisMicroseconds'] > 0,
  ProfileEvents['DistributedIndexAnalysisMissingParts'],
  ProfileEvents['DistributedIndexAnalysisScheduledReplicas'] > 0
)
from system.query_log
where
  current_database = currentDatabase()
  and event_date >= yesterday() AND event_time >= now() - 600
  and type = 'QueryFinish'
  and query_kind = 'Select'
  and is_initial_query
  and has(Settings, 'distributed_index_analysis')
  -- The limited query is excluded: replicas check the rows limit against their own subset of
  -- parts, so whether some replica fails (and its parts are resolved on the initiator) depends
  -- on the distribution of parts over replicas
  and Settings['max_rows_to_read'] = '0'
  and endsWith(log_comment, '-' || currentDatabase())
order by event_time_microseconds;

drop table test_dia_proj;
