-- Tags: no-parallel
-- no-parallel: drops the (instance-wide) query condition cache

-- A lightweight UPDATE is applied as a patch ahead of the WHERE filter, so a granule can become
-- fully non-matching only because of the patch. Recording such granules under the predicate's hash
-- would poison the query condition cache for a later apply_patch_parts = 0 query.
-- Companion of the PREWHERE cases in 03229_query_condition_cache_on_fly_mutations.

DROP TABLE IF EXISTS t_qcc_patch_where;
SET use_query_condition_cache = 1;
-- The cache is populated per replica, so the assertions below need a single one.
SET enable_parallel_replicas = 0;

-- auto_statistics_types is randomized per run; pinned so the cache assertions cannot depend on it.
CREATE TABLE t_qcc_patch_where (id UInt64, v UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1, enable_block_number_column = 1, enable_block_offset_column = 1,
         auto_statistics_types = '';

INSERT INTO t_qcc_patch_where SELECT number, number FROM numbers(100);

SYSTEM STOP MERGES t_qcc_patch_where;
ALTER TABLE t_qcc_patch_where UPDATE v = 0 WHERE id >= 50 SETTINGS alter_update_mode = 'lightweight_force', enable_lightweight_update = 1, mutations_sync = 1;

SYSTEM DROP QUERY CONDITION CACHE;

-- apply_patch_parts = 1 first: the patch sets v = 0 for ids >= 50, so no row has v >= 50 -> 0.
SELECT count() FROM t_qcc_patch_where WHERE v >= 50 SETTINGS apply_patch_parts = 1, optimize_move_to_prewhere = 0, query_plan_optimize_prewhere = 0;
-- apply_patch_parts = 0 must ignore the patch -> ids 50..99 -> 50.
SELECT count() FROM t_qcc_patch_where WHERE v >= 50 SETTINGS apply_patch_parts = 0, optimize_move_to_prewhere = 0, query_plan_optimize_prewhere = 0;

-- `v % 7 = 3000` matches no row and cannot be answered from the index, so every granule read is
-- emptied by the filter: with the patch ignored that is recorded, with the patch applied it is not.
SYSTEM DROP QUERY CONDITION CACHE;
SELECT count() FROM t_qcc_patch_where WHERE v % 7 = 3000 SETTINGS apply_patch_parts = 1, optimize_move_to_prewhere = 0, query_plan_optimize_prewhere = 0;
SELECT count() FROM system.query_condition_cache;
SYSTEM DROP QUERY CONDITION CACHE;
SELECT count() FROM t_qcc_patch_where WHERE v % 7 = 3000 SETTINGS apply_patch_parts = 0, optimize_move_to_prewhere = 0, query_plan_optimize_prewhere = 0;
SELECT count() > 0 FROM system.query_condition_cache;

DROP TABLE t_qcc_patch_where;
