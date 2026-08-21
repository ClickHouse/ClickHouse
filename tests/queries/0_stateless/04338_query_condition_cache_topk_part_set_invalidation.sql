-- Tags: long, no-parallel, no-parallel-replicas
-- Tag no-parallel: Messes with internal cache
-- Tag long: needs ~1.5M rows across two partitions for the TopK threshold to drop
--   whole granules of the unchanged part, so on the slower S3 + sanitizer
--   configuration a single run takes ~180s and crosses the flaky-check "test runs
--   too long" threshold; the data volume is structurally required by the
--   part-set-invalidation scenario this test checks.
--
-- Correctness regression for the Query Condition Cache (QCC) on the
-- `ORDER BY <column> LIMIT n` (TopK) plan: a granule-skip decision recorded for
-- one part depends on the TopK threshold, which is computed from rows of *all*
-- parts the query reads. The QCC key is `(table_uuid, part_name, condition_hash)`,
-- so when another part is dropped or mutated, the entry of an *unchanged* part
-- still matches the key while the decision it stores is stale. The TopK salt
-- therefore includes a hash of the whole part-set snapshot
-- (`ReadFromMergeTree::setTopKColumn`); this test checks that changing the part
-- set really invalidates the cached decisions.

SET allow_experimental_analyzer = 1;
SET use_query_condition_cache = 1;
SET use_top_k_dynamic_filtering = 1;
SET use_skip_indexes_for_top_k = 1;
SET query_plan_max_limit_for_top_k_optimization = 1000;
-- Keep the `WHERE` predicate as a separate `FilterStep` above `ReadFromMergeTree`
-- so the dynamic-filtering branch of `tryOptimizeTopK` applies and the WHERE
-- condition is the one written into the query condition cache.
SET optimize_move_to_prewhere = 0;
SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;
SET parallel_replicas_local_plan = 1;
-- Deterministic read so the warm run reliably populates the cache: single thread,
-- pinned chunk size, no random mark-range splitting (see
-- `04320_query_condition_cache_topk_correctness` for the full rationale).
SET max_threads = 1;
SET max_block_size = 8192;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0.;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab (p UInt8, id UInt32, k UInt32, w UInt32) ENGINE = MergeTree
PARTITION BY p ORDER BY id
SETTINGS index_granularity = 64,
         min_bytes_for_wide_part = 0,
         min_bytes_for_full_part_storage = 0,
         add_minmax_index_for_numeric_columns = 0;

-- Two partitions = two parts, and partition 0 sorts first in the read order.
--
-- Partition 0 holds the `w = 7` rows with the largest `k` (`k` in
-- [500000, 1000000)), so the warm run reads it first and the running threshold
-- of `ORDER BY k DESC LIMIT 5` settles at `k = 995007`.
--
-- Partition 1 interleaves two kinds of rows in every granule (`id` is a per-row
-- hash, so they are scattered uniformly):
--   * "victims" (even `number`): `k` in [0, 500000), `w` matching every 1000th
--     row -- each `k` is *below* the threshold, so `__topKFilter` drops them
--     before the `WHERE` filter ever sees them;
--   * "decoys" (odd `number`): `k` >= 1500000 but `w = 1` -- they survive
--     `__topKFilter` at any threshold, so every chunk of partition 1 reaches the
--     `WHERE` filter non-empty, and `w = 7` then reduces it to zero rows.
-- Net effect: the warm run records *all* granules of partition 1 as skippable
-- under the TopK-salted WHERE key, although they contain `w = 7` rows -- those
-- were merely below a threshold that partition 0's rows established.
INSERT INTO tab
SELECT 0, intHash32(number), number, number % 1000
FROM numbers(500000, 500000)
SETTINGS max_insert_threads = 1, max_insert_block_size = 2_000_000, min_insert_block_size_rows = 2_000_000;

INSERT INTO tab
SELECT 1,
       intHash32(number),
       if(number % 2 = 0, intDiv(number, 2), 1500000 + number),
       if(number % 2 = 0, intDiv(number, 2) % 1000, 1)
FROM numbers(1_000_000)
SETTINGS max_insert_threads = 1, max_insert_block_size = 2_000_000, min_insert_block_size_rows = 2_000_000;

SYSTEM CLEAR QUERY CONDITION CACHE;

SELECT '--- DESC LIMIT 5 ground truth (QCC off): top rows all come from partition 0';
SELECT k FROM tab WHERE w = 7 ORDER BY k DESC LIMIT 5 SETTINGS use_query_condition_cache = 0;

SELECT '--- Warm run records partition 1 granules as skippable under the TopK salt';
SELECT k FROM tab WHERE w = 7 ORDER BY k DESC LIMIT 5 SETTINGS log_comment = '04338_warm';
SELECT count() > 0 FROM system.query_condition_cache;

SELECT '--- Cached run, same part set: must match ground truth';
SELECT k FROM tab WHERE w = 7 ORDER BY k DESC LIMIT 5 SETTINGS log_comment = '04338_cached';

-- Correctness alone can stay green while the cache silently stops firing, so
-- prove the cache is actually *used* via the ProfileEvents in system.query_log:
-- the warm run finds nothing cached (0 hits), the cached run reuses entries (>0).
SYSTEM FLUSH LOGS query_log;
SELECT '--- QCC ProfileEvents (warm has 0 hits, cached has hits)';
SELECT ProfileEvents['QueryConditionCacheHits'] = 0
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '04338_warm' AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC LIMIT 1;
SELECT ProfileEvents['QueryConditionCacheHits'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '04338_cached' AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC LIMIT 1;

-- Dropping partition 0 removes the rows the threshold was computed from. The new
-- top-5 lives entirely in partition 1, whose part is unchanged (same name) and
-- whose granules the warm run recorded as skippable. Reusing those entries would
-- return an empty result; the part-set hash in the TopK salt must invalidate them.
ALTER TABLE tab DROP PARTITION 0;

SELECT '--- After DROP PARTITION: ground truth (QCC off) comes from partition 1';
SELECT k FROM tab WHERE w = 7 ORDER BY k DESC LIMIT 5 SETTINGS use_query_condition_cache = 0;
SELECT '--- QCC on must not reuse granule decisions recorded under the old part set';
SELECT k FROM tab WHERE w = 7 ORDER BY k DESC LIMIT 5 SETTINGS log_comment = '04338_afterdrop';

-- Invalidation seen through ProfileEvents: because the part-set snapshot changed,
-- the TopK-salted key changed too, so this run finds no matching entry (0 hits)
-- even though the part `partition 1` and its plan parameters are unchanged.
SYSTEM FLUSH LOGS query_log;
SELECT '--- QCC ProfileEvents after DROP PARTITION: no hits (key invalidated)';
SELECT ProfileEvents['QueryConditionCacheHits'] = 0
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '04338_afterdrop' AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC LIMIT 1;

-- `condition_hash` folds the sort direction, so a warm `ORDER BY k DESC` entry
-- must not be reused for `ORDER BY k ASC`: the direction change is a fresh key.
-- First prove the DESC warm-up really populated a TopK entry (a rerun hits it);
-- otherwise the ASC 0-hits below would be a miss for the wrong reason (no entry
-- at all on the one-part post-drop dataset), leaving the direction fold untested.
SYSTEM CLEAR QUERY CONDITION CACHE;
SELECT k FROM tab WHERE w = 7 ORDER BY k DESC LIMIT 5 SETTINGS log_comment = '04338_descwarm' FORMAT Null;
SELECT k FROM tab WHERE w = 7 ORDER BY k DESC LIMIT 5 SETTINGS log_comment = '04338_descrerun' FORMAT Null;
SYSTEM FLUSH LOGS query_log;
SELECT '--- DESC rerun hits its warm entry (proves direction DESC populated QCC)';
SELECT ProfileEvents['QueryConditionCacheHits'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '04338_descrerun' AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC LIMIT 1;

SELECT '--- ASC after warming DESC: distinct direction key, must not hit';
SELECT k FROM tab WHERE w = 7 ORDER BY k ASC LIMIT 5 SETTINGS use_query_condition_cache = 0;
SELECT k FROM tab WHERE w = 7 ORDER BY k ASC LIMIT 5 SETTINGS log_comment = '04338_asc';
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['QueryConditionCacheHits'] = 0
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '04338_asc' AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC LIMIT 1;

-- A negative `LIMIT` is not a TopK plan (it becomes a `NegativeLimit` step with no
-- `__topKFilter`), so it never engages the TopK-salted QCC path -- it must return
-- the correct rows and neither reuse nor poison the TopK entries. Warm a positive
-- TopK entry, run the negative-limit query (0 hits: it does not reuse the entry),
-- then rerun the positive query (still hits: the negative run did not poison it).
SYSTEM CLEAR QUERY CONDITION CACHE;
SELECT k FROM tab WHERE w = 7 ORDER BY k DESC LIMIT 5 SETTINGS log_comment = '04338_neg_warm' FORMAT Null;
SELECT '--- Negative LIMIT: correct result, TopK-QCC path does not engage';
SELECT k FROM tab WHERE w = 7 ORDER BY k DESC LIMIT -5 SETTINGS use_query_condition_cache = 0;
SELECT k FROM tab WHERE w = 7 ORDER BY k DESC LIMIT -5 SETTINGS log_comment = '04338_neg';
-- The positive rerun after the negative query must return the SAME rows as the
-- QCC-off ground truth, not merely report a cache hit. A FORMAT Null rerun would
-- stay green even if 04338_neg (legitimately 0 hits) overwrote the positive TopK
-- entry through a write-side key collision, so materialize the cached rerun and
-- compare it row-for-row against the QCC-off ground truth.
SELECT '--- Positive TopK ground truth (QCC off) after the negative run';
SELECT k FROM tab WHERE w = 7 ORDER BY k DESC LIMIT 5 SETTINGS use_query_condition_cache = 0;
SELECT '--- Positive TopK cached rerun: must match ground truth (not poisoned)';
SELECT k FROM tab WHERE w = 7 ORDER BY k DESC LIMIT 5 SETTINGS log_comment = '04338_neg_pos2';
SYSTEM FLUSH LOGS query_log;
SELECT '--- Negative LIMIT does not reuse the TopK entry (0 hits)';
SELECT ProfileEvents['QueryConditionCacheHits'] = 0
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '04338_neg' AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC LIMIT 1;
SELECT '--- Positive TopK query still hits after the negative run (not poisoned)';
SELECT ProfileEvents['QueryConditionCacheHits'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '04338_neg_pos2' AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC LIMIT 1;

DROP TABLE tab;
