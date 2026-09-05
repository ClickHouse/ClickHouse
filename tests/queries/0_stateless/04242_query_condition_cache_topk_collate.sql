-- Tags: no-parallel, no-parallel-replicas, no-fasttest
-- Tag no-parallel: Messes with internal cache.
-- Tag no-fasttest: COLLATE requires ICU, which is not available in the Fast test build.
--
-- Companion to `04217_query_condition_cache_topk.sql`: verify that the QCC key
-- is partitioned by `COLLATE` locale so two re-runs with the same locale reuse
-- the same entry, while a different locale produces a fresh entry.

SET allow_experimental_analyzer = 1;
SET use_query_condition_cache = 1;
SET use_top_k_dynamic_filtering = 1;
SET use_skip_indexes_for_top_k = 1;
SET query_plan_max_limit_for_top_k_optimization = 1000;
SET optimize_move_to_prewhere = 0;
SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;
SET parallel_replicas_local_plan = 1;
-- Variable-length sort column (`String`) goes through `tryOptimizeTopK`'s dynamic-filtering
-- branch only when this opt-in is set; the `COLLATE` assertions below rely on it.
SET use_top_k_dynamic_filtering_for_variable_length_types = 1;

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (id UInt32, s String, v UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64,
         min_bytes_for_wide_part = 0,
         min_bytes_for_full_part_storage = 0,
         add_minmax_index_for_numeric_columns = 0;

INSERT INTO tab
SELECT rand(), toString(number), number
FROM numbers(1_000_000);

SYSTEM CLEAR QUERY CONDITION CACHE;
SELECT count() FROM system.query_condition_cache;

SELECT '--- Same COLLATE locale re-runs reuse the same QCC entry';
SELECT s FROM tab WHERE v = 10000 ORDER BY s ASC COLLATE 'en_US' LIMIT 5 FORMAT Null;
SELECT count() FROM system.query_condition_cache;
SELECT s FROM tab WHERE v = 10000 ORDER BY s ASC COLLATE 'en_US' LIMIT 5 FORMAT Null;
SELECT count() FROM system.query_condition_cache;

SELECT '--- Different COLLATE locale writes a separate entry';
SELECT s FROM tab WHERE v = 10000 ORDER BY s ASC COLLATE 'fr' LIMIT 5 FORMAT Null;
SELECT count() FROM system.query_condition_cache;

DROP TABLE tab;
