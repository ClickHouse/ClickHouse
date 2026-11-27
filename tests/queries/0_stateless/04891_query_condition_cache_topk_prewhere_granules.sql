-- Tags: long, no-parallel, no-parallel-replicas
-- Tag no-parallel: Messes with internal cache
-- Tag long: does a ~100k-row insert and several full-part TopK scans, matching the
--   sibling `04320_query_condition_cache_topk_correctness` test.
--
-- Granules fully filtered by the dynamic `__topKFilter` PREWHERE of an
-- `ORDER BY <column> LIMIT n` (TopK) read are recorded in the query condition cache
-- under a key salted with the TopK plan parameters, so a repeat run of the same plan
-- skips them at the mark-selection stage (issue #114639). The sibling test
-- `04320_query_condition_cache_topk_correctness` covers the WHERE `FilterStep` write
-- path; this test isolates the PREWHERE write path by using a query with no WHERE at
-- all: any query condition cache activity can then only come from the PREWHERE side.

SET allow_experimental_analyzer = 1;
SET use_query_condition_cache = 1;
SET use_query_condition_cache_for_top_k = 1;
SET use_top_k_dynamic_filtering = 1;
SET query_plan_max_limit_for_top_k_optimization = 1000;
-- Parallel replicas split the plan into a different shape and do extra QCC lookups.
SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;
SET parallel_replicas_local_plan = 1;
-- A granule is recorded as skippable only when the PREWHERE leaves none of its rows,
-- which depends on the running threshold each reading thread sees. Read with a single
-- thread over a single part so the threshold trajectory is deterministic, and pin the
-- block size so chunks align to granules (see the sibling test for the full rationale).
SET max_threads = 1;
SET max_block_size = 8192;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0.;
-- Do not let seek-gap merging read the granules the cache has dropped: the selected-marks
-- assertion below counts on the dropped range staying dropped.
SET merge_tree_min_rows_for_seek = 0;
SET merge_tree_min_bytes_for_seek = 0;

DROP TABLE IF EXISTS tab;

-- The sort column `k` is scattered across the part with no correlation to the primary
-- key, except that all rows with the smallest `k` (`k < 8192`) sit in the first
-- 8192-row chunk (`id = k`). After the first chunk is sorted, the threshold tracker is
-- armed at its final value (`k = 4` for `ASC LIMIT 5`), so every later chunk loses all
-- of its rows at the `__topKFilter` PREWHERE and its granules are recorded as skippable.
-- 4294959104 is `2^32 - 8192`, so the ids stay within UInt32.
CREATE TABLE tab (id UInt32, k UInt32, w UInt8) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64,
         min_bytes_for_wide_part = 0,
         min_bytes_for_full_part_storage = 0,
         add_minmax_index_for_numeric_columns = 0;

INSERT INTO tab SELECT
    if(number < 8192, number, 8192 + intHash32(number) % 4294959104),
    number,
    number >= 8192
FROM numbers(100_000)
SETTINGS max_insert_threads = 1, max_insert_block_size = 2_000_000, min_insert_block_size_rows = 2_000_000;

SYSTEM CLEAR QUERY CONDITION CACHE;

SELECT '--- QCC starts empty';
SELECT count() FROM system.query_condition_cache;

SELECT '--- ASC LIMIT 5: ground truth (QCC off)';
SELECT k FROM tab ORDER BY k ASC LIMIT 5 SETTINGS use_query_condition_cache = 0;

SELECT '--- ASC LIMIT 5: the first run records PREWHERE-filtered granules';
SELECT k FROM tab ORDER BY k ASC LIMIT 5 SETTINGS log_comment = '04891_first';
-- There is no WHERE, so any entry must have been written by the PREWHERE write path.
SELECT count() > 0 FROM system.query_condition_cache;

SELECT '--- ASC LIMIT 5: the second run consults them, must match ground truth';
SELECT k FROM tab ORDER BY k ASC LIMIT 5 SETTINGS log_comment = '04891_second';

-- The threshold is learned after WHERE, while __topKFilter runs in PREWHERE. Priming the
-- cache with w = 0 therefore records every later granule as skippable; that PREWHERE entry
-- must not be reused for w = 1, whose five smallest rows all live in those granules.
SYSTEM CLEAR QUERY CONDITION CACHE;
SELECT '--- Different WHERE must not reuse the PREWHERE granule decisions: prime w = 0';
SELECT k FROM tab WHERE w = 0 ORDER BY k ASC LIMIT 5;
SELECT '--- Different WHERE must not reuse the PREWHERE granule decisions';
SELECT k FROM tab WHERE w = 1 ORDER BY k ASC LIMIT 5;

-- The post-PREWHERE predicate determines the dynamic threshold. A non-deterministic
-- predicate cannot safely share PREWHERE TopK granule decisions between executions,
-- even when its structural DAG hash is unchanged.
SYSTEM CLEAR QUERY CONDITION CACHE;
SELECT '--- Non-deterministic WHERE must not write PREWHERE TopK cache entries';
SELECT k FROM tab WHERE rand() % 2 = 0 ORDER BY k ASC LIMIT 5 FORMAT Null;
SELECT count() FROM system.query_condition_cache;

-- A query with an explicit PREWHERE and a separate WHERE must use the exact key shape
-- written by the PREWHERE path: combine(prewhere_hash, combine(topk_hash, where_hash)).
SYSTEM CLEAR QUERY CONDITION CACHE;
SELECT '--- Mixed PREWHERE and WHERE: first run records granule decisions';
SELECT k FROM tab PREWHERE w = 0 WHERE k >= 0 ORDER BY k ASC LIMIT 5 SETTINGS log_comment = '04891_mixed_first';
SELECT '--- Mixed PREWHERE and WHERE: second run must reuse them';
SELECT k FROM tab PREWHERE w = 0 WHERE k >= 0 ORDER BY k ASC LIMIT 5 SETTINGS log_comment = '04891_mixed_second';

-- `PREWHERE` `TopK` entries are shared by all users. Prime them as an unrestricted user,
-- then verify a restrictive row policy cannot reuse the cached decisions before it
-- hides the first chunk: the policy user's smallest visible keys are in later granules.
SYSTEM CLEAR QUERY CONDITION CACHE;
SELECT '--- Row policy must not reuse unrestricted PREWHERE granule decisions: prime';
SELECT k FROM tab ORDER BY k ASC LIMIT 5;
DROP USER IF EXISTS user_04891;
CREATE USER user_04891;
GRANT SELECT ON tab TO user_04891;
DROP ROW POLICY IF EXISTS policy_04891 ON tab;
CREATE ROW POLICY policy_04891 ON tab FOR SELECT USING w = 1 TO user_04891;
CREATE ROW POLICY policy_04891_default ON tab FOR SELECT USING 1 TO default;
SELECT '--- Row policy must not reuse unrestricted PREWHERE granule decisions';
EXECUTE AS user_04891 SELECT k FROM tab ORDER BY k ASC LIMIT 5;
DROP ROW POLICY policy_04891 ON tab;
DROP ROW POLICY policy_04891_default ON tab;
DROP USER user_04891;

-- The WHERE carrier must have the same isolation. A policy is evaluated before WHERE,
-- therefore its result must neither write nor reuse a WHERE cache entry that omits the
-- policy predicate. Without this isolation, the first query would cache the first chunk
-- as empty and make the unrestricted query incorrectly skip keys 0 through 4.
SYSTEM CLEAR QUERY CONDITION CACHE;
DROP USER IF EXISTS user_04891;
CREATE USER user_04891;
GRANT SELECT ON tab TO user_04891;
DROP ROW POLICY IF EXISTS policy_04891 ON tab;
CREATE ROW POLICY policy_04891 ON tab FOR SELECT USING w = 1 TO user_04891;
CREATE ROW POLICY policy_04891_default ON tab FOR SELECT USING 1 TO default;
SELECT '--- Row policy must not poison WHERE cache entries';
EXECUTE AS user_04891 SELECT k FROM tab WHERE k >= 0 ORDER BY k ASC LIMIT 5;
SELECT '--- Unrestricted user must not reuse row-policy WHERE cache entries';
SELECT k FROM tab WHERE k >= 0 ORDER BY k ASC LIMIT 5;
DROP ROW POLICY policy_04891 ON tab;
DROP ROW POLICY policy_04891_default ON tab;
DROP USER user_04891;

-- The opposite sort direction needs the rows with the *largest* `k`, which live
-- exclusively in granules the ASC plan just recorded as skippable (every row outside
-- the first chunk has `k >= 8192`). The `__topKFilter(k)` PREWHERE condition of both
-- plans hashes identically, so without the TopK plan salt on the cache key the DESC
-- run would reuse the ASC verdicts and lose all of its rows.
SELECT '--- DESC LIMIT 5: must not reuse the granule decisions of the ASC plan';
SELECT k FROM tab ORDER BY k DESC LIMIT 5 SETTINGS use_query_condition_cache = 0;
SELECT '---';
SELECT k FROM tab ORDER BY k DESC LIMIT 5;

SYSTEM FLUSH LOGS query_log;

-- The second ASC run must have skipped granules at the mark-selection stage: strictly
-- fewer selected marks than the first run (which reads the whole part, ~1563 marks vs
-- ~130 once the recorded granules are dropped), and a query condition cache hit.
SELECT '--- The second run reads fewer marks';
SELECT
    second.marks < first.marks / 2,
    second.qcc_hits >= 1
FROM
    (SELECT ProfileEvents['SelectedMarks'] AS marks
     FROM system.query_log
     WHERE current_database = currentDatabase() AND log_comment = '04891_first' AND type = 'QueryFinish'
     ORDER BY event_time_microseconds DESC LIMIT 1) AS first,
    (SELECT ProfileEvents['SelectedMarks'] AS marks, ProfileEvents['QueryConditionCacheHits'] AS qcc_hits
     FROM system.query_log
     WHERE current_database = currentDatabase() AND log_comment = '04891_second' AND type = 'QueryFinish'
     ORDER BY event_time_microseconds DESC LIMIT 1) AS second;

SELECT '--- The mixed PREWHERE and WHERE run hits the query condition cache';
SELECT ProfileEvents['QueryConditionCacheHits'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '04891_mixed_second' AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC LIMIT 1;

DROP TABLE tab;
