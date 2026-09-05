-- Tags: long, no-parallel, no-parallel-replicas
-- Tag no-parallel: Messes with internal cache
-- Tag long: needs ~1M rows for the QCC to populate (a granule-spanning chunk must be
--   fully filtered before the LIMIT cancels the pipeline), so on the slower S3 +
--   sanitizer configuration a single run takes ~180s and crosses the flaky-check
--   "test runs too long" threshold; the data volume is structurally required and
--   can't be cut without losing the cache-population assertions below.
--
-- Test that the Query Condition Cache (QCC) is populated for queries that go
-- through the `ORDER BY <column> LIMIT n` (TopK) plan, and that QCC entries
-- are partitioned by the TopK plan parameters (column, type, LIMIT, direction,
-- num_sort_columns) so a re-run with the same plan reuses the cached granule
-- decisions while a re-run with a different plan stores a fresh entry instead.

SET allow_experimental_analyzer = 1;
SET use_query_condition_cache = 1;
SET use_top_k_dynamic_filtering = 1;
SET use_skip_indexes_for_top_k = 1;
SET query_plan_max_limit_for_top_k_optimization = 1000;
-- Keep the `WHERE` predicate as a separate `FilterStep` above `ReadFromMergeTree` so
-- the dynamic-filtering branch of `tryOptimizeTopK` applies (it bails out if
-- `getPrewhereInfo()` is already set on the read step before TopK runs) and the
-- WHERE condition is the one written into the query condition cache.
SET optimize_move_to_prewhere = 0;
-- Parallel replicas split the plan into a different shape and do extra QCC lookups
-- that this test doesn't expect.
SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;
SET parallel_replicas_local_plan = 1;

DROP TABLE IF EXISTS tab;

-- `id` is the primary key (random per row), `v1` / `v2` are non-PK columns the
-- TopK optimization can target. 1 million rows: the QCC doesn't cache anything
-- for less data than that.
CREATE TABLE tab (id UInt32, v1 UInt32, v2 UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64,
         min_bytes_for_wide_part = 0,
         min_bytes_for_full_part_storage = 0,
         add_minmax_index_for_numeric_columns = 0;

INSERT INTO tab SELECT rand(), number, number FROM numbers(1_000_000);

SELECT '--- QCC starts empty';
SYSTEM CLEAR QUERY CONDITION CACHE;
SELECT count() FROM system.query_condition_cache;

SELECT '--- Same TopK plan re-runs reuse the same QCC entry';
SELECT v1 FROM tab WHERE v2 = 10000 ORDER BY v1 ASC LIMIT 5 FORMAT Null;
SELECT count() FROM system.query_condition_cache;
SELECT v1 FROM tab WHERE v2 = 10000 ORDER BY v1 ASC LIMIT 5 FORMAT Null;
SELECT count() FROM system.query_condition_cache;

SELECT '--- Different LIMIT writes a separate entry';
SELECT v1 FROM tab WHERE v2 = 10000 ORDER BY v1 ASC LIMIT 7 FORMAT Null;
SELECT count() FROM system.query_condition_cache;

SELECT '--- Different sort direction writes a separate entry';
SELECT v1 FROM tab WHERE v2 = 10000 ORDER BY v1 DESC LIMIT 5 FORMAT Null;
SELECT count() FROM system.query_condition_cache;

SELECT '--- Different sort column writes a separate entry';
SELECT v2 FROM tab WHERE v2 = 10000 ORDER BY v2 ASC LIMIT 5 FORMAT Null;
SELECT count() FROM system.query_condition_cache;

DROP TABLE tab;

SELECT '';
SELECT '--- Focused assertions for NULLS direction ---';

DROP TABLE IF EXISTS tab2;
CREATE TABLE tab2 (id UInt32, n Nullable(UInt32), v UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64,
         min_bytes_for_wide_part = 0,
         min_bytes_for_full_part_storage = 0,
         add_minmax_index_for_numeric_columns = 0;

-- Half the rows have NULL in `n`, so `NULLS FIRST/LAST` is observably different on the data.
INSERT INTO tab2
SELECT rand(), if(number % 2 = 0, number, NULL), number
FROM numbers(1_000_000);

SYSTEM CLEAR QUERY CONDITION CACHE;
SELECT count() FROM system.query_condition_cache;

SELECT '--- Same NULLS direction re-runs reuse the same QCC entry';
SELECT n FROM tab2 WHERE v = 10000 ORDER BY n ASC NULLS FIRST LIMIT 5 FORMAT Null;
SELECT count() FROM system.query_condition_cache;
SELECT n FROM tab2 WHERE v = 10000 ORDER BY n ASC NULLS FIRST LIMIT 5 FORMAT Null;
SELECT count() FROM system.query_condition_cache;

SELECT '--- Different NULLS direction writes a separate entry';
SELECT n FROM tab2 WHERE v = 10000 ORDER BY n ASC NULLS LAST LIMIT 5 FORMAT Null;
SELECT count() FROM system.query_condition_cache;

DROP TABLE tab2;
