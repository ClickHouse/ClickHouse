-- Tags: no-parallel-replicas, no-parallel
-- Tag no-parallel: messes with the server-level query condition cache

-- Regression test mirroring 04275_104781_qcc_prewhere_skip_index_attribution
-- but for sparsity `data_read` pruning. When
-- `use_sparsity_info_for_pruning = 'data_read'` the sparsity reader sits ahead
-- of PREWHERE and drops whole marks via `canSkipMark`. The reader chain must
-- report this through `canSkipAnyMark`, otherwise those marks get attributed
-- to the PREWHERE predicate and poison the query condition cache.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_sparsity_prewhere;

CREATE TABLE t_sparsity_prewhere (project_id String, x UInt32)
ENGINE = MergeTree ORDER BY project_id
SETTINGS min_bytes_for_wide_part = 0,
         index_granularity = 8192,
         ratio_of_defaults_for_sparse_serialization = 0.5;

-- All rows share `project_id = 'P1'`. `x` is sparse and clustered: the first
-- 300000 rows have x=0 and the rest have x=1. After insert the leading granules
-- are entirely x=0, so `data_read` pruning drops them for `WHERE x != 0`.
INSERT INTO t_sparsity_prewhere
SELECT 'P1', if(number < 300000, 0, 1)::UInt32 FROM numbers(500000);

SYSTEM DROP QUERY CONDITION CACHE;

SELECT 'truth', count() FROM t_sparsity_prewhere WHERE project_id = 'P1'
SETTINGS use_query_condition_cache = 1, use_sparsity_info_for_pruning = 'data_read',
         optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1,
         optimize_use_implicit_projections = 0,
         optimize_trivial_count_with_sparsity_filter = 0;

-- Trigger: PREWHERE on the primary key prefix + WHERE on the sparse column.
-- Sparsity drops the all-x=0 marks at read time. Before the fix those marks were
-- attributed to the PREWHERE predicate `project_id = 'P1'` in the cache, so the
-- follow-up `count() WHERE project_id = 'P1'` returned 205088 instead of 500000.
SELECT project_id FROM t_sparsity_prewhere
PREWHERE project_id = 'P1'
WHERE x != 0
FORMAT Null
SETTINGS use_query_condition_cache = 1, use_sparsity_info_for_pruning = 'data_read',
         optimize_trivial_count_with_sparsity_filter = 0;

-- With the fix, this returns the full 500000. Without the fix it returned 205088
-- because the cache wrongly said marks dropped by sparsity did not match
-- `project_id = 'P1'`.
SELECT 'after_trigger', count() FROM t_sparsity_prewhere WHERE project_id = 'P1'
SETTINGS use_query_condition_cache = 1, use_sparsity_info_for_pruning = 'data_read',
         optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1,
         optimize_use_implicit_projections = 0,
         optimize_trivial_count_with_sparsity_filter = 0;

-- Sanity: cache-off path must match `truth`.
SELECT 'cache_off', count() FROM t_sparsity_prewhere WHERE project_id = 'P1'
SETTINGS use_query_condition_cache = 0, use_sparsity_info_for_pruning = 'data_read',
         optimize_use_implicit_projections = 0,
         optimize_trivial_count_with_sparsity_filter = 0;

-- The other sparsity modes share the same cache. Their answers under the same
-- PREWHERE hash must also match `truth`.
SELECT 'cache_on_sparsity_off', count() FROM t_sparsity_prewhere WHERE project_id = 'P1'
SETTINGS use_query_condition_cache = 1, use_sparsity_info_for_pruning = 'off',
         optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1,
         optimize_use_implicit_projections = 0,
         optimize_trivial_count_with_sparsity_filter = 0;

SELECT 'cache_on_sparsity_planning', count() FROM t_sparsity_prewhere WHERE project_id = 'P1'
SETTINGS use_query_condition_cache = 1, use_sparsity_info_for_pruning = 'planning',
         optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1,
         optimize_use_implicit_projections = 0,
         optimize_trivial_count_with_sparsity_filter = 0;

DROP TABLE t_sparsity_prewhere;
