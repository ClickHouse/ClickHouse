-- Tags: no-parallel, no-release
-- Tag no-parallel: Messes with internal cache
-- Tag no-release: Checks fields in system.query_condition_cache which are not available in release builds
-- add_minmax_index_for_numeric_columns=0: Would use indices instead of the projections that we want to test

SET use_statistics_for_part_pruning = 0; -- Prevent auto_statistics_types from pruning parts before query condition cache

-- { echo ON }

set enable_analyzer = 1;
set parallel_replicas_local_plan = 1, parallel_replicas_support_projection = 1, optimize_aggregation_in_order = 0;
set optimize_use_projections = 1, optimize_use_implicit_projections = 1, optimize_use_projection_filtering = 1;
-- With `use_skip_indexes_on_data_read` on (the default) the reader-side cache write is suppressed.
-- The stateless test profile sets it to 0 while a plain local server leaves it at 1, so without pinning
-- it the entries below depend on where the test runs.
set use_skip_indexes_on_data_read = 0;

drop table if exists t;

create table t (i int, j int, projection p (select * order by j)) engine MergeTree order by tuple()
settings index_granularity = 1, add_minmax_index_for_numeric_columns=0, max_bytes_to_merge_at_max_space_in_pool = 1; -- disable merge

-- The following data is constructed in a way to verifies that query condition
-- cache no longer has key collisions for projection parts
insert into t select 20, number from numbers(10);

insert into t select 1, number + 1 from numbers(10);

system clear query condition cache;

select j from t where j > 3 and i = 20 order by j settings max_threads = 1, use_query_condition_cache = 1, query_condition_cache_store_conditions_as_plaintext = 1;

-- One entry per part per key space: the reader records row-level exclusions under the plain
-- condition hash, index analysis records its own under a hash salted with the skip-index profile
-- (issue #108519). Two keys for one predicate is not a collision - one records what evaluating
-- the predicate on rows ruled out, the other what index analysis of the same predicate ruled out.
-- The goal of the test is to assert that different parts do not share a cache entry.
select part_name from system.query_condition_cache order by part_name;

select j from t where j > 3 and i = 20 order by j settings max_threads = 1, use_query_condition_cache = 1, query_condition_cache_store_conditions_as_plaintext = 1;

drop table t;
