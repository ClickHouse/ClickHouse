-- Tags: no-parallel-replicas
-- no-parallel-replicas: the query condition cache is populated per replica

-- The `__topKFilter` PREWHERE of an `ORDER BY ... LIMIT` read drops granules based on a threshold
-- derived from the rows of all parts. A read that applies an on-fly mutation or a patch part to one part
-- must not record granules of another part as empty under a threshold tightened by the rewritten rows:
-- a later read that does not apply the mutation would skip that part. Issue #122564.

SET use_query_condition_cache = 1;
SET use_query_condition_cache_for_top_k = 1;
SET use_top_k_dynamic_filtering = 1;
SET query_plan_max_limit_for_top_k_optimization = 1000;
SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;
SET max_threads = 1;
SET max_block_size = 8192;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;
SET merge_tree_min_rows_for_seek = 0;
SET merge_tree_min_bytes_for_seek = 0;
SET mutations_sync = 0;

SELECT 'ALTER UPDATE applied on fly';

DROP TABLE IF EXISTS t_qcc_topk_mut;
CREATE TABLE t_qcc_topk_mut (id UInt32, k UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64, min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, add_minmax_index_for_numeric_columns = 0;

SYSTEM STOP MERGES t_qcc_topk_mut;
INSERT INTO t_qcc_topk_mut SELECT number, 1000000 + number FROM numbers(8192);
-- Stays pending, so only on-fly reads see k = 0..4 in the first part.
ALTER TABLE t_qcc_topk_mut UPDATE k = id WHERE id < 5;
-- Inserted after the mutation, which therefore does not apply to this part.
INSERT INTO t_qcc_topk_mut SELECT 100000 + number, 1000 + number FROM numbers(100000);

SELECT k FROM t_qcc_topk_mut ORDER BY k LIMIT 5 SETTINGS apply_mutations_on_fly = 1;
SELECT k FROM t_qcc_topk_mut ORDER BY k LIMIT 5 SETTINGS apply_mutations_on_fly = 0;

SELECT 'after KILL MUTATION';
KILL MUTATION WHERE database = currentDatabase() AND table = 't_qcc_topk_mut' SYNC FORMAT Null;
SELECT k FROM t_qcc_topk_mut ORDER BY k LIMIT 5 SETTINGS apply_mutations_on_fly = 1;

DROP TABLE t_qcc_topk_mut;

SELECT 'patch parts';

DROP TABLE IF EXISTS t_qcc_topk_patch;
CREATE TABLE t_qcc_topk_patch (id UInt32, k UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64, min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, add_minmax_index_for_numeric_columns = 0,
    enable_block_number_column = 1, enable_block_offset_column = 1;

SYSTEM STOP MERGES t_qcc_topk_patch;
INSERT INTO t_qcc_topk_patch SELECT number, 1000000 + number FROM numbers(8192);
INSERT INTO t_qcc_topk_patch SELECT 100000 + number, 1000 + number FROM numbers(100000);
SET enable_lightweight_update = 1;
UPDATE t_qcc_topk_patch SET k = id WHERE id < 5;

SELECT k FROM t_qcc_topk_patch ORDER BY k LIMIT 5 SETTINGS apply_patch_parts = 1;
SELECT k FROM t_qcc_topk_patch ORDER BY k LIMIT 5 SETTINGS apply_patch_parts = 0;

DROP TABLE t_qcc_topk_patch;
