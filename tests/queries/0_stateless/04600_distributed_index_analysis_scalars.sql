-- Tags: no-random-merge-tree-settings, no-random-settings, long
-- - no-random-merge-tree-settings -- may change number of parts

drop table if exists test_200_parts;
create table test_200_parts (key Int, value Int) engine=MergeTree() order by key partition by key % 200 settings distributed_index_analysis_min_parts_to_activate=0, distributed_index_analysis_min_indexes_bytes_to_activate=0;
system stop merges test_200_parts;
insert into test_200_parts select number, number*100 from numbers(1e6) settings max_partitions_per_insert_block=200, max_block_size=1e6;

drop table if exists test_20_parts;
create table test_20_parts (key Int, value Int) engine=MergeTree() order by key partition by key % 20 settings distributed_index_analysis_min_parts_to_activate=0, distributed_index_analysis_min_indexes_bytes_to_activate=0;
system stop merges test_20_parts;
insert into test_20_parts select number, number*100 from numbers(1e3) settings max_partitions_per_insert_block=20, max_block_size=1e6;

set allow_experimental_parallel_reading_from_replicas=0;
set parallel_replicas_for_non_replicated_merge_tree=1;
set parallel_replicas_index_analysis_only_on_coordinator=1;
set parallel_replicas_local_plan=1;
set distributed_index_analysis=1;
set max_parallel_replicas=2;
set allow_experimental_analyzer=1;
--- Ignore warnings when replica does not respond, and analysis is done on initiator
set send_logs_level='error';

-- { echo }
select sum(key) from test_200_parts settings cluster_for_parallel_replicas='test_cluster_one_shard_two_replicas';
select sum(key) from test_20_parts settings cluster_for_parallel_replicas='test_cluster_one_shard_two_replicas';

-- { echoOff }
system flush logs query_log;
select normalizeQuery(query), tables
from system.query_log
where
  event_date >= yesterday() AND event_time >= now() - 600
  and type != 'QueryStart'
  and query_kind = 'Select'
  and endsWith(log_comment, '-' || currentDatabase()) -- "current_database = currentDatabase()" cannot be used, remote queries does not have it
order by event_time_microseconds;
