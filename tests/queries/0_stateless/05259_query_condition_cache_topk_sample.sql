-- Tags: no-parallel, no-parallel-replicas
-- Tag no-parallel: Messes with internal cache
--
-- An `ORDER BY ... LIMIT n` (TopK) read records in the query condition cache the granules its dynamic
-- `__topKFilter` rejected: none of their rows is in the top N of the whole table. A `SAMPLE` read computes
-- its top N from a subset of the rows, which can lie in exactly those granules, so it must not reuse such
-- entries. It still returns what it returns with the cache off.

SET allow_experimental_analyzer = 1;
SET use_query_condition_cache = 1;
SET use_query_condition_cache_for_top_k = 1;
SET use_top_k_dynamic_filtering = 1;
SET query_plan_max_limit_for_top_k_optimization = 1000;
SET optimize_move_to_prewhere = 0;
-- Reading in `k` order stops before the threshold rejects whole granules, so nothing would be recorded.
SET optimize_read_in_order = 0;
SET optimize_use_projections = 0;
SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;
SET max_threads = 1;
SET max_block_size = 8192;

DROP TABLE IF EXISTS tab_topk_sample;

-- The leading `g` spreads the sampled rows over the whole read order. Sampling on the leading key column
-- alone would select the granules read first, which set the threshold and are never excluded.
CREATE TABLE tab_topk_sample
(
    g UInt8,
    id UInt64,
    k UInt64,
    v UInt64
)
ENGINE = MergeTree
ORDER BY (g, intHash32(id))
SAMPLE BY intHash32(id)
SETTINGS index_granularity = 64,
         min_bytes_for_wide_part = 0,
         min_bytes_for_full_part_storage = 0,
         add_minmax_index_for_numeric_columns = 0;

INSERT INTO tab_topk_sample SELECT number % 10, number, cityHash64(number) % 1000000000, number FROM numbers(200000);

-- Query condition cache entries are keyed by part; a background merge between the queries would drop them.
SYSTEM STOP MERGES tab_topk_sample;
SYSTEM CLEAR QUERY CONDITION CACHE;

SELECT '--- A warm unsampled TopK read reuses the granules the first run excluded';

SELECT k FROM tab_topk_sample WHERE v >= 0 ORDER BY k LIMIT 5 FORMAT Null SETTINGS log_comment = '05259_topk_prime';
SELECT k FROM tab_topk_sample WHERE v >= 0 ORDER BY k LIMIT 5 FORMAT Null SETTINGS log_comment = '05259_topk_warm';

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, ProfileEvents['QueryConditionCacheHits'] > 0
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment IN ('05259_topk_prime', '05259_topk_warm')
ORDER BY event_time_microseconds;

SELECT '--- A sampled TopK read returns the same rows as with the cache off';

DROP TABLE IF EXISTS res_topk_sample_cached;
DROP TABLE IF EXISTS res_topk_sample_uncached;
CREATE TABLE res_topk_sample_cached (k UInt64) ENGINE = Memory;
CREATE TABLE res_topk_sample_uncached (k UInt64) ENGINE = Memory;

INSERT INTO res_topk_sample_uncached SELECT k FROM tab_topk_sample SAMPLE 1 / 10 WHERE v >= 0 ORDER BY k LIMIT 5 SETTINGS use_query_condition_cache = 0;
INSERT INTO res_topk_sample_cached SELECT k FROM tab_topk_sample SAMPLE 1 / 10 WHERE v >= 0 ORDER BY k LIMIT 5;

SELECT count() FROM res_topk_sample_uncached;
SELECT (SELECT groupArraySorted(10)(k) FROM res_topk_sample_cached) = (SELECT groupArraySorted(10)(k) FROM res_topk_sample_uncached);

DROP TABLE res_topk_sample_cached;
DROP TABLE res_topk_sample_uncached;
DROP TABLE tab_topk_sample;
