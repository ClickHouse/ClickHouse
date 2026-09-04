-- Tags: long, no-parallel, no-parallel-replicas
-- Tag no-parallel: Messes with internal cache
-- Tag long: needs ~1M rows for the QCC to populate.
--
-- Companion of `04628_query_condition_cache_topk_default_off`, covering plans whose
-- `ReadFromMergeTree` step is *cloned* after the TopK marker was attached by `tryOptimizeTopK`.
--
-- `setTopKColumn` records the marker (`top_k_filter_info`) and, with
-- `use_query_condition_cache_for_top_k = 0`, also clears `allow_query_condition_cache`. Both must
-- survive `ReadFromMergeTree::clone`, otherwise a cloned read looks like a plain read and can
-- consult (`filterPartsByQueryConditionCache`) or populate (index-analysis exclusions in
-- `selectRangesToRead`, row-level entries from the reader) the query condition cache under the
-- unsalted condition hash. There are two cloning paths: `materializeQueryPlanReferences` for a
-- common subplan reference (produced by decorrelating a correlated subquery with
-- `correlated_subqueries_use_in_memory_buffer = 0`) and `cloneSubtree` for a parallel-replicas
-- plan fragment.
--
-- Note that the plan shapes below currently clone only the read subtree *below* the sorting and
-- limit steps, which is exactly the part `tryOptimizeTopK` never stamps, so the clones are plain
-- reads today and legitimately keep using the cache. The test therefore pins the observable
-- contract of these shapes - results and cache use - so that a future change which starts
-- producing cloned TopK reads cannot silently reintroduce unsalted reuse.

SET allow_experimental_analyzer = 1;
SET use_query_condition_cache = 1;
-- Pin the gate to its default value: this test covers the default-off contract.
SET use_query_condition_cache_for_top_k = 0;
SET use_top_k_dynamic_filtering = 1;
SET use_skip_indexes_for_top_k = 1;
SET query_plan_max_limit_for_top_k_optimization = 1000;
SET optimize_move_to_prewhere = 0;
SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;
SET parallel_replicas_local_plan = 1;
SET max_threads = 1;
SET allow_experimental_correlated_subqueries = 1;
-- The last assertion below is a positive control that depends on the decorrelated join keeping the
-- `tab` read on the side where the `v2 IN (...)` predicate becomes a `FilterStep` above
-- `ReadFromMergeTree`, which is what primes and then reuses a query condition cache entry. Join
-- order randomization replaces the real statistics with random cardinalities and can swap the join
-- sides, so the entry is never written and the control reads 0. Pin the join shape - the shapes
-- this test covers are about cloning, not about join ordering.
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 'false';
SET use_hash_table_stats_for_join_reordering = 0;
-- Decorrelation without the in-memory buffer is what emits a `CommonSubplanReferenceStep`, which
-- `materializeQueryPlanReferences` later materializes by cloning the referenced subplan.
SET correlated_subqueries_use_in_memory_buffer = 0;

DROP TABLE IF EXISTS tab;
DROP TABLE IF EXISTS keys;

CREATE TABLE tab (id UInt32, v1 UInt32, v2 UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64,
         min_bytes_for_wide_part = 0,
         min_bytes_for_full_part_storage = 0,
         add_minmax_index_for_numeric_columns = 0;

-- Query condition cache entries are keyed by part, so a background merge of `tab` invalidates every
-- entry primed before it. The positive control below reuses an entry across two queries, which a
-- merge landing in between turns into a miss - stop merges to make the reuse deterministic.
SYSTEM STOP MERGES tab;

INSERT INTO tab SELECT number, number, number FROM numbers(1_000_000);

CREATE TABLE keys (k UInt32) ENGINE = MergeTree ORDER BY k;
INSERT INTO keys SELECT number * 1000 FROM numbers(1000);

SELECT '--- A cloned common subplan reference returns the same rows as the plain TopK read';

-- Both queries select the same rows; the second one carries a correlated `EXISTS` whose
-- decorrelation makes the plan contain a materialized (cloned) common subplan reference.
SELECT v1 FROM tab WHERE v2 IN (10000, 11000, 12000) ORDER BY v1 ASC LIMIT 5;
SELECT v1 FROM tab WHERE v2 IN (10000, 11000, 12000) AND EXISTS (SELECT 1 FROM keys WHERE keys.k = tab.id) ORDER BY v1 ASC LIMIT 5;

SELECT '--- The TopK read of the same predicate still writes no QCC entry';
SYSTEM CLEAR QUERY CONDITION CACHE;

SELECT v1 FROM tab WHERE v2 IN (10000, 11000, 12000) ORDER BY v1 ASC LIMIT 5 FORMAT Null SETTINGS log_comment = '04647_topk_write_1';
SELECT v1 FROM tab WHERE v2 IN (10000, 11000, 12000) ORDER BY v1 ASC LIMIT 5 FORMAT Null SETTINGS log_comment = '04647_topk_write_2';

-- Expected: 0 entries - every write side is gated off for a TopK read while the gate is off.
SELECT count() FROM system.query_condition_cache;

SYSTEM FLUSH LOGS query_log;

-- Column: (any QCC hit). Expected: 0 for both runs.
SELECT
    log_comment,
    ProfileEvents['QueryConditionCacheHits'] > 0
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment IN ('04647_topk_write_1', '04647_topk_write_2')
ORDER BY event_time_microseconds;

SELECT '--- A TopK read must not reuse an entry primed by the cloned-subplan query';
SYSTEM CLEAR QUERY CONDITION CACHE;

-- The cloned subplan read is a plain read, so it primes and then reuses its own entry as usual.
SELECT v1 FROM tab WHERE v2 IN (10000, 11000, 12000) AND EXISTS (SELECT 1 FROM keys WHERE keys.k = tab.id) ORDER BY v1 ASC LIMIT 5 FORMAT Null SETTINGS log_comment = '04647_clone_prime';
-- The TopK read of the same predicate must not consult that entry with the gate off.
SELECT v1 FROM tab WHERE v2 IN (10000, 11000, 12000) ORDER BY v1 ASC LIMIT 5 FORMAT Null SETTINGS log_comment = '04647_topk_after_clone_prime';
SELECT v1 FROM tab WHERE v2 IN (10000, 11000, 12000) AND EXISTS (SELECT 1 FROM keys WHERE keys.k = tab.id) ORDER BY v1 ASC LIMIT 5 FORMAT Null SETTINGS log_comment = '04647_clone_reuse';

SYSTEM FLUSH LOGS query_log;

-- Column: (any QCC hit). Expected: prime = 0, topk-after-prime = 0, clone-reuse = 1.
SELECT
    log_comment,
    ProfileEvents['QueryConditionCacheHits'] > 0
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment IN ('04647_clone_prime', '04647_topk_after_clone_prime', '04647_clone_reuse')
ORDER BY event_time_microseconds;

DROP TABLE keys;
DROP TABLE tab;
