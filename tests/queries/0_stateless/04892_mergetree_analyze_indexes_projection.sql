-- Tags: no-random-merge-tree-settings, no-random-settings
-- - no-random-merge-tree-settings -- may change number of parts and granules

drop table if exists test_mtai_proj;
create table test_mtai_proj (key Int, value Int) engine=MergeTree() order by key
settings distributed_index_analysis_min_parts_to_activate=0, distributed_index_analysis_min_indexes_bytes_to_activate=0;

system stop merges test_mtai_proj;
-- The first part is created before the projection is added, so it has no projection part
insert into test_mtai_proj select number, number from numbers(0, 100000);
alter table test_mtai_proj add projection prj_value (select key, value order by value);
insert into test_mtai_proj select number, number from numbers(100000, 100000);
insert into test_mtai_proj select number, number from numbers(200000, 100000);

set allow_experimental_parallel_reading_from_replicas=0;
set cluster_for_parallel_replicas='';
set max_parallel_replicas=100;
set distributed_index_analysis_for_non_shared_merge_tree=1;
set use_query_condition_cache=0;

-- { echo }
-- Projection parts are identified by the parent part name, ranges are pruned by the projection sorting key;
-- the part that has no projection part is absent from the result
select * from mergeTreeAnalyzeIndexes(currentDatabase(), test_mtai_proj, value = 150000, ['all_1_1_0', 'all_2_2_0', 'all_3_3_0'], 'prj_value') order by part_name;
-- Without the projection the table primary key cannot prune by value
select * from mergeTreeAnalyzeIndexes(currentDatabase(), test_mtai_proj, value = 150000, ['all_2_2_0']) order by part_name;
select * from mergeTreeAnalyzeIndexes(currentDatabase(), test_mtai_proj, value = 150000, ['all_2_2_0'], 'no_such_projection'); -- { serverError BAD_ARGUMENTS }
select * from mergeTreeAnalyzeIndexes(currentDatabase(), test_mtai_proj, value = 150000, ['all_2_2_0'], 'prj_value', 'vector_search_index_analysis', [], 'extra'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
--- Ignore warnings when replica does not respond, and analysis is done on initiator
set send_logs_level='error';
-- The first matching row comes from the table (its part has no projection part), the other two are read via the projection
select count(), sum(key) from test_mtai_proj where value in (50000, 150000, 250000) settings distributed_index_analysis=0;
select count(), sum(key) from test_mtai_proj where value in (50000, 150000, 250000) settings cluster_for_parallel_replicas='parallel_replicas', distributed_index_analysis=1;

-- { echoOff }

drop table test_mtai_proj;
