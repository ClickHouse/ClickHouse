-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

-- { echo ON }

set enable_analyzer = 1;
set enable_parallel_replicas = 0;
set optimize_use_projection_filtering = 1;

drop table if exists t;

create table t (i int, j int, projection p (select * order by j)) engine MergeTree order by tuple()
settings index_granularity = 1, max_bytes_to_merge_at_max_space_in_pool = 1; -- disable merge

-- The following data is constructed in a way to verifies that query condition
-- cache no longer has key collisions for projection parts
insert into t select 20, number from numbers(10);

insert into t select 1, number + 1 from numbers(10);

system drop query condition cache;

select j from t where j > 3 and i = 20 order by j settings max_threads = 1, use_query_condition_cache = 1, query_condition_cache_store_conditions_as_plaintext = 1;

select part_name from system.query_condition_cache order by part_name;

select j from t where j > 3 and i = 20 order by j settings max_threads = 1, use_query_condition_cache = 1, query_condition_cache_store_conditions_as_plaintext = 1;

drop table t;
