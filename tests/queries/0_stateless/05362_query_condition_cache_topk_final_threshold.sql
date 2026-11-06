-- Tags: no-parallel, no-parallel-replicas
-- Tag no-parallel: Messes with internal cache

-- A TopK read (`ORDER BY k LIMIT n`) records in the query condition cache the granules whose rows all sort strictly
-- beyond the final threshold, not only the granules `__topKFilter` emptied against the running one.

SET use_query_condition_cache = 1;
SET use_query_condition_cache_for_top_k = 1;
SET use_top_k_dynamic_filtering = 1;
SET query_plan_max_limit_for_top_k_optimization = 1000;
SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;
SET parallel_replicas_local_plan = 1;
-- One thread and one granule per block, so the threshold trajectory is deterministic.
SET max_threads = 1;
SET max_block_size = 64;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0.;
SET merge_tree_min_rows_for_seek = 0;
SET merge_tree_min_bytes_for_seek = 0;

DROP TABLE IF EXISTS tab;

-- `k` decreases along the part: each granule has rows below the threshold published by the granules before it, so
-- `__topKFilter` empties none of the 100 granules. Only the last one holds rows of the result.
CREATE TABLE tab (id UInt32, k UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64, index_granularity_bytes = 10485760, min_bytes_for_wide_part = 0,
         min_bytes_for_full_part_storage = 0, add_minmax_index_for_numeric_columns = 0;

INSERT INTO tab SELECT number, 1000000 - number FROM numbers(6400)
SETTINGS max_insert_threads = 1, max_insert_block_size = 1000000, min_insert_block_size_rows = 1000000;

SYSTEM CLEAR QUERY CONDITION CACHE;

SELECT '--- reference';
SELECT k FROM tab ORDER BY k LIMIT 5 SETTINGS use_query_condition_cache = 0, use_top_k_dynamic_filtering = 0;
SELECT '--- first run';
SELECT k FROM tab ORDER BY k LIMIT 5 SETTINGS log_comment = '05362-asc-first';
SELECT '--- repeated run reads the last granule only';
SELECT k FROM tab ORDER BY k LIMIT 5 SETTINGS log_comment = '05362-asc-repeat';

-- Ties: the threshold value 0 is in every granule, so no granule can be dropped.
DROP TABLE IF EXISTS tab_ties;
CREATE TABLE tab_ties (id UInt32, k UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64, index_granularity_bytes = 10485760, min_bytes_for_wide_part = 0,
         min_bytes_for_full_part_storage = 0, add_minmax_index_for_numeric_columns = 0;
INSERT INTO tab_ties SELECT number, if(number % 64 = 63, 0, 1000000 - number) FROM numbers(6400)
SETTINGS max_insert_threads = 1, max_insert_block_size = 1000000, min_insert_block_size_rows = 1000000;

SELECT '--- ties: reference';
SELECT k, count() FROM (SELECT k FROM tab_ties ORDER BY k LIMIT 5) GROUP BY k SETTINGS use_query_condition_cache = 0, use_top_k_dynamic_filtering = 0;
SELECT '--- ties: first and repeated runs';
SELECT k, count() FROM (SELECT k FROM tab_ties ORDER BY k LIMIT 5) GROUP BY k SETTINGS log_comment = '05362-ties-first';
SELECT k, count() FROM (SELECT k FROM tab_ties ORDER BY k LIMIT 5) GROUP BY k SETTINGS log_comment = '05362-ties-repeat';

-- A String sort column in descending order, compared by unsigned bytes: each granule has one UTF-8 value, which sorts
-- after all ASCII values, and 63 ASCII values that grow along the part. The last 5 granules hold the result.
DROP TABLE IF EXISTS tab_str;
CREATE TABLE tab_str (id UInt32, s String) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64, index_granularity_bytes = 10485760, min_bytes_for_wide_part = 0,
         min_bytes_for_full_part_storage = 0, add_minmax_index_for_numeric_columns = 0;
INSERT INTO tab_str SELECT number, concat(if(number % 64 = 0, 'я', 'z'), leftPad(toString(number), 6, '0')) FROM numbers(6400)
SETTINGS max_insert_threads = 1, max_insert_block_size = 1000000, min_insert_block_size_rows = 1000000;

SELECT '--- strings: reference';
SELECT s FROM tab_str ORDER BY s DESC LIMIT 5 SETTINGS use_query_condition_cache = 0, use_top_k_dynamic_filtering = 0;
SELECT '--- strings: first and repeated runs';
SELECT s FROM tab_str ORDER BY s DESC LIMIT 5 SETTINGS log_comment = '05362-str-first';
SELECT s FROM tab_str ORDER BY s DESC LIMIT 5 SETTINGS log_comment = '05362-str-repeat';

SYSTEM FLUSH LOGS query_log;
SELECT '--- rows read';
SELECT log_comment, read_rows
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '05362-%'
ORDER BY event_time_microseconds;

DROP TABLE tab;
DROP TABLE tab_ties;
DROP TABLE tab_str;
