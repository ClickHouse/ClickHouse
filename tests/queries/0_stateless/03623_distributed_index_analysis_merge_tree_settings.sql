-- Tags: long, no-parallel

set allow_experimental_parallel_reading_from_replicas=0;
set cluster_for_parallel_replicas='test_cluster_one_shard_two_replicas';

drop table if exists dist_idx;
create table dist_idx (key Int, value Int) engine=MergeTree() order by key settings distributed_index_analysis_min_parts_to_activate=0, distributed_index_analysis_min_indexes_bytes_to_activate=0;
insert into dist_idx select number, number*100 from numbers(1e6);
select sum(key) from dist_idx settings distributed_index_analysis=1 format Null;
drop table dist_idx;

drop table if exists no_dist_idx_not_enough_indexes;
create table no_dist_idx_not_enough_indexes (key Int, value Int) engine=MergeTree() order by key settings distributed_index_analysis_min_parts_to_activate=0, distributed_index_analysis_min_indexes_bytes_to_activate=1e12;
insert into no_dist_idx_not_enough_indexes select number, number*100 from numbers(1e6);
select sum(key) from no_dist_idx_not_enough_indexes settings distributed_index_analysis=1 format Null;
drop table no_dist_idx_not_enough_indexes;

drop table if exists no_dist_idx_min_not_enough_parts;
create table no_dist_idx_min_not_enough_parts (key Int, value Int) engine=MergeTree() order by key settings distributed_index_analysis_min_parts_to_activate=1e9, distributed_index_analysis_min_indexes_bytes_to_activate=0;
insert into no_dist_idx_min_not_enough_parts select number, number*100 from numbers(1e6);
select sum(key) from no_dist_idx_min_not_enough_parts settings distributed_index_analysis=1 format Null;
drop table no_dist_idx_min_not_enough_parts;

drop table if exists no_dist_idx;
create table no_dist_idx (key Int, value Int) engine=MergeTree() order by key settings distributed_index_analysis_min_parts_to_activate=1e9, distributed_index_analysis_min_indexes_bytes_to_activate=1e12;
insert into no_dist_idx select number, number*100 from numbers(1e6);
select sum(key) from no_dist_idx settings distributed_index_analysis=1 format Null;
drop table no_dist_idx;

drop table if exists dist_idx_parts;
create table dist_idx_parts (key Int, value Int) engine=MergeTree() order by key settings merge_selector_base=1000, index_granularity=8192, min_bytes_for_wide_part=1e9, index_granularity_bytes=10e6, distributed_index_analysis_min_parts_to_activate=10, distributed_index_analysis_min_indexes_bytes_to_activate=0;
system stop merges dist_idx_parts;
insert into dist_idx_parts select number, number*100 from numbers(1e6) settings max_block_size=10000, min_insert_block_size_rows=10000, max_insert_threads=1;
select sum(key) from dist_idx_parts settings distributed_index_analysis=1 format Null;
drop table dist_idx_parts;

drop table if exists dist_idx_pk_size;
create table dist_idx_pk_size (key String, value String) engine=MergeTree() order by (key, value) settings index_granularity=200, min_bytes_for_wide_part=0, index_granularity_bytes=10e6, distributed_index_analysis_min_parts_to_activate=0, distributed_index_analysis_min_indexes_bytes_to_activate=500e3, compress_primary_key=0;
system stop merges dist_idx_pk_size;
insert into dist_idx_pk_size select number::String, repeat('a', 100) from numbers(1e6);
select table, sum(primary_key_size) from system.parts where database = currentDatabase() AND table = 'dist_idx_pk_size' group by 1;
select key from dist_idx_pk_size settings distributed_index_analysis=1 format Null;
drop table dist_idx_pk_size;

drop table if exists dist_idx_skipping_idx_size;
create table dist_idx_skipping_idx_size (key String, value String, index key_val_idx (key, value) type set(100000)) engine=MergeTree() settings index_granularity=100000, min_bytes_for_wide_part=0, index_granularity_bytes=10e6, distributed_index_analysis_min_parts_to_activate=0, distributed_index_analysis_min_indexes_bytes_to_activate='10M';
system stop merges dist_idx_skipping_idx_size;
insert into dist_idx_skipping_idx_size select number::String, repeat('a', 100) from numbers(1e6);
select table, sum(data_uncompressed_bytes) from system.data_skipping_indices where database = currentDatabase() AND table = 'dist_idx_skipping_idx_size' group by 1;
select key from dist_idx_skipping_idx_size settings distributed_index_analysis=1 format Null;
drop table dist_idx_skipping_idx_size;

system flush logs query_log;
select tables, ProfileEvents['DistributedIndexAnalysisMicroseconds'] > 0
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
