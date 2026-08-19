-- Tags: no-random-merge-tree-settings, no-random-settings
-- - no-random-merge-tree-settings -- test relies on the number of rows per granule

drop table if exists test_dia_offset;
create table test_dia_offset (key Int) engine=MergeTree() order by key
settings distributed_index_analysis_min_parts_to_activate=0, distributed_index_analysis_min_indexes_bytes_to_activate=0;

system stop merges test_dia_offset;
insert into test_dia_offset select number from numbers(0, 100000);
insert into test_dia_offset select number from numbers(100000, 100000);
insert into test_dia_offset select number from numbers(200000, 100000);

set allow_experimental_parallel_reading_from_replicas=0;
set cluster_for_parallel_replicas='';
set max_parallel_replicas=100;
set distributed_index_analysis_for_non_shared_merge_tree=1;
set use_query_condition_cache=0;

--- Ignore warnings when replica does not respond, and analysis is done on initiator
set send_logs_level='error';

-- { echo }
-- Filters over part offsets are applied on the initiator (remote replicas cannot evaluate them),
-- so the marks stay pruned under the distributed analysis - see `SelectedMarks` below, and the
-- rows limit that does not fit a full scan
select count(), sum(key) from test_dia_offset where _part_offset >= 90000 settings max_rows_to_read=60000, distributed_index_analysis=0;
select count(), sum(key) from test_dia_offset where _part_offset >= 90000 settings max_rows_to_read=60000, cluster_for_parallel_replicas='parallel_replicas', distributed_index_analysis=1;
select count(), sum(key) from test_dia_offset where _part_offset >= 90000 settings cluster_for_parallel_replicas='parallel_replicas', distributed_index_analysis=1;
select count(), sum(key) from test_dia_offset where _part_offset + _part_starting_offset >= 250000 settings max_rows_to_read=60000, distributed_index_analysis=0;
select count(), sum(key) from test_dia_offset where _part_offset + _part_starting_offset >= 250000 settings max_rows_to_read=60000, cluster_for_parallel_replicas='parallel_replicas', distributed_index_analysis=1;
select count(), sum(key) from test_dia_offset where _part_offset + _part_starting_offset >= 250000 settings cluster_for_parallel_replicas='parallel_replicas', distributed_index_analysis=1;
-- Control query, to prove that the check below can see the distributed analysis when it happens
select count(), sum(key) from test_dia_offset where key >= 0 settings cluster_for_parallel_replicas='parallel_replicas', distributed_index_analysis=1;

-- { echoOff }

drop table if exists test_dia_offset_in;
create table test_dia_offset_in (user_id Int, val Int) engine=MergeTree() order by val
settings distributed_index_analysis_min_parts_to_activate=0, distributed_index_analysis_min_indexes_bytes_to_activate=0;
system stop merges test_dia_offset_in;
insert into test_dia_offset_in select if(number in (11111, 22222), 42, number + 1000000), number from numbers(0, 100000);
insert into test_dia_offset_in select if(number in (133333, 144444), 42, number + 1000000), number from numbers(100000, 100000);
insert into test_dia_offset_in select if(number = 255555, 42, number + 1000000), number from numbers(200000, 100000);

drop table if exists test_dia_offset_prj;
create table test_dia_offset_prj (user_id Int, val Int, projection prj (select user_id, _part_offset order by user_id)) engine=MergeTree() order by val
settings distributed_index_analysis_min_parts_to_activate=0, distributed_index_analysis_min_indexes_bytes_to_activate=0;
system stop merges test_dia_offset_prj;
insert into test_dia_offset_prj select if(number in (11111, 22222), 42, number + 1000000), number from numbers(0, 100000);
insert into test_dia_offset_prj select if(number in (133333, 144444), 42, number + 1000000), number from numbers(100000, 100000);
insert into test_dia_offset_prj select if(number = 255555, 42, number + 1000000), number from numbers(200000, 100000);

-- { echo }
-- Total offsets from a subquery: the set is applied on the initiator (`SelectedMarks` below
-- cover both the inner and the outer reading)
select count() from test_dia_offset_in where _part_starting_offset + _part_offset in (select _part_starting_offset + _part_offset from test_dia_offset_in where user_id = 42) settings enable_shared_storage_snapshot_in_query=1, distributed_index_analysis=0;
select count() from test_dia_offset_in where _part_starting_offset + _part_offset in (select _part_starting_offset + _part_offset from test_dia_offset_in where user_id = 42) settings enable_shared_storage_snapshot_in_query=1, cluster_for_parallel_replicas='parallel_replicas', distributed_index_analysis=1;
-- The same with a projection over `_part_offset`: the subquery reads it (3 marks of the total 8),
-- the outer query still reads the table (the projection cannot serve the total offsets filter better)
select count() from test_dia_offset_prj where _part_starting_offset + _part_offset in (select _part_starting_offset + _part_offset from test_dia_offset_prj where user_id = 42) settings enable_shared_storage_snapshot_in_query=1, distributed_index_analysis=0;
select count() from test_dia_offset_prj where _part_starting_offset + _part_offset in (select _part_starting_offset + _part_offset from test_dia_offset_prj where user_id = 42) settings enable_shared_storage_snapshot_in_query=1, cluster_for_parallel_replicas='parallel_replicas', distributed_index_analysis=1;

-- { echoOff }
system flush logs query_log;
select format(
  'distributed_index_analysis={}, DistributedIndexAnalysisMicroseconds>0={}, SelectedMarks={}',
  Settings['distributed_index_analysis'],
  ProfileEvents['DistributedIndexAnalysisMicroseconds'] > 0,
  ProfileEvents['SelectedMarks']
)
from system.query_log
where
  current_database = currentDatabase()
  and event_date >= yesterday() AND event_time >= now() - 600
  and type = 'QueryFinish'
  and query_kind = 'Select'
  and is_initial_query
  and has(Settings, 'distributed_index_analysis')
  and endsWith(log_comment, '-' || currentDatabase())
order by event_time_microseconds;

drop table test_dia_offset;
drop table test_dia_offset_in;
drop table test_dia_offset_prj;
