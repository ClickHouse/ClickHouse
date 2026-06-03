-- Tags: no-parallel, no-parallel-replicas
-- Tag no-parallel: Messes with internal cache
--
-- Correctness regression for the Query Condition Cache (QCC) on the
-- `ORDER BY <column> LIMIT n` (TopK) plan. The companion test
-- `04217_query_condition_cache_topk` only checks that QCC entries are
-- *partitioned* by the TopK plan (entry counts). This test checks the part that
-- made QCC unsafe for TopK in the first place: the `__topKFilter` prewhere drops
-- rows before the `WHERE` `FilterStep` writes a QCC entry, so the granules the
-- filter marks as "no match" are only non-matching under that plan's threshold.
-- A cached read with the same plan must still return exactly the rows a
-- non-cached read returns, and a re-run with a different `LIMIT` / sort direction
-- must not reuse a row set computed under a different (tighter) threshold — the
-- TopK salt on the cache key keeps the plans separate.

SET allow_experimental_analyzer = 1;
SET use_query_condition_cache = 1;
SET use_top_k_dynamic_filtering = 1;
SET use_skip_indexes_for_top_k = 1;
SET query_plan_max_limit_for_top_k_optimization = 1000;
-- Keep the `WHERE` predicate as a separate `FilterStep` above `ReadFromMergeTree`
-- so the dynamic-filtering branch of `tryOptimizeTopK` applies and the WHERE
-- condition is the one written into the query condition cache.
SET optimize_move_to_prewhere = 0;
-- Parallel replicas split the plan into a different shape and do extra QCC lookups.
SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;
SET parallel_replicas_local_plan = 1;

DROP TABLE IF EXISTS tab;

-- `id` is random, so the sort column `k` is scattered across granules: the TopK
-- threshold can't drop whole granules via the minmax index, instead `__topKFilter`
-- drops individual rows (those with `k` beyond the running threshold) inside every
-- granule it reads. `w` matches every 1000th row, so most granules have no `w = 7`
-- row left after `__topKFilter`, and the `WHERE` filter records them as skippable
-- in the QCC. 1 million rows: the QCC doesn't cache anything for less data than that.
CREATE TABLE tab (id UInt32, k UInt32, w UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64,
         min_bytes_for_wide_part = 0,
         min_bytes_for_full_part_storage = 0,
         add_minmax_index_for_numeric_columns = 0;

INSERT INTO tab SELECT rand(), number, number % 1000 FROM numbers(1_000_000);

SYSTEM CLEAR QUERY CONDITION CACHE;

SELECT '--- QCC starts empty';
SELECT count() FROM system.query_condition_cache;

SELECT '--- ASC LIMIT 5: ground truth (QCC off)';
SELECT k FROM tab WHERE w = 7 ORDER BY k ASC LIMIT 5 SETTINGS use_query_condition_cache = 0;

SELECT '--- ASC LIMIT 5: first run writes a QCC entry under the active TopK filter';
SELECT k FROM tab WHERE w = 7 ORDER BY k ASC LIMIT 5;
SELECT count() > 0 FROM system.query_condition_cache;

SELECT '--- ASC LIMIT 5: second run reads cached granule decisions, must match ground truth';
SELECT k FROM tab WHERE w = 7 ORDER BY k ASC LIMIT 5;

-- A larger LIMIT raises the threshold, so it needs `w = 7` rows whose `k` the
-- LIMIT 5 plan dropped (and whose granules the LIMIT 5 plan marked skippable). The
-- salt gives this plan a fresh entry, so the result stays complete.
SELECT '--- ASC LIMIT 12: QCC on must match ground truth (needs rows the LIMIT 5 plan dropped)';
SELECT k FROM tab WHERE w = 7 ORDER BY k ASC LIMIT 12 SETTINGS use_query_condition_cache = 0;
SELECT '---';
SELECT k FROM tab WHERE w = 7 ORDER BY k ASC LIMIT 12;

-- The opposite sort direction needs the `w = 7` rows with the *largest* `k`, which
-- the ASC plans dropped via `__topKFilter` and recorded as skippable. Reusing an
-- ASC entry here would lose every row; the salt gives DESC a fresh entry, so the
-- result is complete — this is the regression that proves the salt is load-bearing.
SELECT '--- DESC LIMIT 5: ground truth (QCC off)';
SELECT k FROM tab WHERE w = 7 ORDER BY k DESC LIMIT 5 SETTINGS use_query_condition_cache = 0;
SELECT '---';
SELECT k FROM tab WHERE w = 7 ORDER BY k DESC LIMIT 5;

DROP TABLE tab;
