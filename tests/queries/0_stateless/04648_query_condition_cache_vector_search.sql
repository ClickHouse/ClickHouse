-- Tags: no-fasttest, no-ordinary-database, no-parallel, no-parallel-replicas
-- no-fasttest: the Fast test build has no USearch, so `vector_similarity` is not a registered index
--   type there and CREATE TABLE would fail
-- no-ordinary-database: an Ordinary database gives the table a nil UUID, and the query condition
--   cache is never written for a nil table UUID
-- no-parallel: drops the (instance-wide) query condition cache
-- no-parallel-replicas: the query condition cache is populated per replica, and the vector-search
--   optimization is disabled for parallel replicas, so the poisoning is not reproducible there

-- A vector-search read narrows every granule to the candidate rows returned by the vector index
-- before the WHERE filter runs. A granule whose candidates all fail the WHERE was therefore
-- recorded as "this WHERE predicate matches nothing", although the predicate was never evaluated
-- on the granule's other rows. A later ordinary query with the same predicate then skipped the
-- granule and silently returned fewer rows.

-- The vector-search optimization needs the analyzer, so the assertions below are
-- vacuous under the `old analyzer` CI variant unless it is pinned here.
SET enable_analyzer = 1;
-- The plan assertions below read the legacy `EXPLAIN` format; the default `pretty` format rewrites
-- the step descriptions.
SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS tab;

-- One part with a single data granule:
--   rows 0, 1   -> vector near the origin (the nearest neighbours), flag = 0 (fail the WHERE)
--   rows 2..299 -> vector far away,                                flag = 1 (must stay findable)
-- `index_granularity` and `index_granularity_bytes` are both randomized by the test runner, and
-- either one splits the part. All 300 rows must land in ONE granule: the two nearest neighbours have
-- to share a granule with the flag = 1 rows, otherwise the index returns candidates from a granule
-- the WHERE does accept and the query below returns rows.
CREATE TABLE tab (id UInt64, flag UInt8, vec Array(Float32),
    INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 3) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8192, index_granularity_bytes = 10485760;

SYSTEM STOP MERGES tab;
-- `materialize_statistics_on_insert` is randomized by the test runner. With statistics materialized,
-- implicit column statistics prune the granule of the last control query below before it is read, so
-- no cache entry is written and the control would stop proving that the guard is narrow.
INSERT INTO tab SELECT
    number,
    if(number < 2, 0, 1),
    if(number < 2, [toFloat32(number) * 0.001, 0., 0.],
                   [toFloat32(5000 + number), toFloat32(5000 + number), toFloat32(5000 + number)])
FROM numbers(300)
SETTINGS materialize_statistics_on_insert = 0;

SELECT 'The table contains 298 rows with flag = 1.';
SELECT count() FROM tab WHERE flag = 1 SETTINGS use_query_condition_cache = 0;

SELECT '--- optimized plan (vector_search_with_rescoring = 0)';

SYSTEM DROP QUERY CONDITION CACHE;

-- Every path that makes the vector-search optimization decline produces an ordinary read, whose
-- output is byte-identical to the intended output below, so the plan is pinned explicitly. The
-- optimized plan reads the vector index and replaces the vector column by the virtual `_distance`
-- column.
SELECT 'The vector index is read and the plan uses `_distance`.';
SELECT countIf(trimLeft(explain) LIKE 'Description: vector_similarity%') > 0, countIf(explain LIKE '%_distance%') > 0
FROM (EXPLAIN header = 1, indexes = 1
    SELECT id FROM tab WHERE flag = 1 ORDER BY L2Distance(vec, [0., 0., 0.]) LIMIT 2
    SETTINGS vector_search_with_rescoring = 0, use_query_condition_cache = 1);

-- The query itself returns no rows: the index returns rows 0 and 1 as the two nearest neighbours and
-- both have flag = 0, so the WHERE removes them. An ordinary read would return the two nearest
-- flag = 1 rows instead, which is why the rows are printed rather than discarded.
-- Both lazy-materialization settings are randomized by the test runner. Without lazy materialization
-- the vector rewrite drops the cache-tagged filter and nothing re-tags it, so this section stops
-- failing on unfixed code. They are pinned so that it keeps proving the non-rescoring write path.
SELECT 'A vector-search query must NOT write to the query condition cache. It returns no rows.';
SELECT id FROM tab WHERE flag = 1 ORDER BY L2Distance(vec, [0., 0., 0.]) LIMIT 2 SETTINGS vector_search_with_rescoring = 0, use_query_condition_cache = 1, query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 10;
SELECT count() FROM system.query_condition_cache;

SELECT 'A later ordinary query must still find all 298 rows.';
SELECT count() FROM tab WHERE flag = 1 SETTINGS use_query_condition_cache = 1;

SELECT '--- rescoring plan (vector_search_with_rescoring = 1)';

SYSTEM DROP QUERY CONDITION CACHE;

-- The rescoring plan reads the vector index too, but keeps the vector column and re-computes the
-- distance for the candidate rows, so `_distance` is deliberately absent here (the same expectation
-- as in 02354_vector_search_rescoring).
SELECT 'The vector index is read and rescoring does not use `_distance`.';
SELECT countIf(trimLeft(explain) LIKE 'Description: vector_similarity%') > 0, countIf(explain LIKE '%_distance%') > 0
FROM (EXPLAIN header = 1, indexes = 1
    SELECT id FROM tab WHERE flag = 1 ORDER BY L2Distance(vec, [0., 0., 0.]) LIMIT 2
    SETTINGS vector_search_with_rescoring = 1, use_query_condition_cache = 1);

SELECT 'A vector-search query must NOT write to the query condition cache. It returns no rows.';
SELECT id FROM tab WHERE flag = 1 ORDER BY L2Distance(vec, [0., 0., 0.]) LIMIT 2 SETTINGS vector_search_with_rescoring = 1, use_query_condition_cache = 1;
SELECT count() FROM system.query_condition_cache;

SELECT 'A later ordinary query must still find all 298 rows.';
SELECT count() FROM tab WHERE flag = 1 SETTINGS use_query_condition_cache = 1;

SELECT '--- the query condition cache stays enabled for reads without the vector optimization';

SYSTEM DROP QUERY CONDITION CACHE;

SELECT 'An ordinary query still writes to the query condition cache.';
SELECT count() FROM tab WHERE flag = 7 SETTINGS use_query_condition_cache = 1 FORMAT Null;
SELECT count() FROM system.query_condition_cache;

SYSTEM DROP QUERY CONDITION CACHE;

-- An explicit PREWHERE turns the vector-search optimization off, so no candidate-row filter runs
-- ahead of the filter and the cache entry is sound. This pins that the guard is narrow.
SELECT 'A vector-search query with an explicit PREWHERE still writes to the query condition cache.';
SELECT id FROM tab PREWHERE flag = 1 ORDER BY L2Distance(vec, [0., 0., 0.]) LIMIT 2 SETTINGS use_query_condition_cache = 1 FORMAT Null;
SELECT count() FROM system.query_condition_cache;

SELECT 'And that entry does not lose rows either.';
SELECT count() FROM tab WHERE flag = 1 SETTINGS use_query_condition_cache = 1;

DROP TABLE tab;
