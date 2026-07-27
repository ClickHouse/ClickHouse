-- Tags: no-parallel, no-parallel-replicas
-- no-parallel: drops the (instance-wide) query condition cache
-- no-parallel-replicas: the query condition cache is populated per replica, and the vector-search
--   optimization is disabled for parallel replicas, so the poisoning is not reproducible there

-- A vector-search read narrows every granule to the candidate rows returned by the vector index
-- before the WHERE filter runs. A granule whose candidates all fail the WHERE was therefore
-- recorded as "this WHERE predicate matches nothing", although the predicate was never evaluated
-- on the granule's other rows. A later ordinary query with the same predicate then skipped the
-- granule and silently returned fewer rows.

-- The vector-search optimization only exists in the new analyzer, so the assertions below are
-- vacuous under the `old analyzer` CI variant unless it is pinned here.
SET enable_analyzer = 1;

DROP TABLE IF EXISTS tab;

-- One part with a single data granule:
--   rows 0, 1   -> vector near the origin (the nearest neighbours), flag = 0 (fail the WHERE)
--   rows 2..299 -> vector far away,                                flag = 1 (must stay findable)
CREATE TABLE tab (id UInt64, flag UInt8, vec Array(Float32),
    INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 3) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id;

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

SELECT 'A vector-search query must NOT write to the query condition cache.';
SELECT id FROM tab WHERE flag = 1 ORDER BY L2Distance(vec, [0., 0., 0.]) LIMIT 2 SETTINGS vector_search_with_rescoring = 0 FORMAT Null;
SELECT count() FROM system.query_condition_cache;

SELECT 'A later ordinary query must still find all 298 rows.';
SELECT count() FROM tab WHERE flag = 1 SETTINGS use_query_condition_cache = 1;

SELECT '--- rescoring plan (vector_search_with_rescoring = 1)';

SYSTEM DROP QUERY CONDITION CACHE;

SELECT 'A vector-search query must NOT write to the query condition cache.';
SELECT id FROM tab WHERE flag = 1 ORDER BY L2Distance(vec, [0., 0., 0.]) LIMIT 2 SETTINGS vector_search_with_rescoring = 1 FORMAT Null;
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
