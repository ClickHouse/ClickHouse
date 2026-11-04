-- Tags: no-fasttest
-- no-fasttest: vector search needs usearch 3rd party library

-- Tests vector search behavior when some vector columns have indexes and others don't.
-- Verifies behavior of filter strategies for both indexed and non-indexed vector columns
-- in the same table.

SET enable_analyzer = 1;
SET parallel_replicas_local_plan = 1;

DROP TABLE IF EXISTS tab;

-- Two vector columns but a vector index is built only on top of one
CREATE TABLE tab (
    key Int32,
    vec1 Array(BFloat16),
    vec2 Array(BFloat16),
    INDEX idx vec1 TYPE vector_similarity('hnsw', 'cosineDistance', 16)
)
ENGINE = MergeTree ORDER BY key
SETTINGS index_granularity = 3;

INSERT INTO tab
SELECT
    number,
    arrayMap(i -> randCanonical(i), range(16)),
    arrayMap(i -> randCanonical(i), range(32))
FROM numbers(100);

-- Test vector search on indexed column (vec1) with different filter strategies
--
-- Expect to use index lookups for auto and postfilter strategies, and PREWHERE
-- filter + brute force distance calculation for the prefilter strategy

SELECT '-- Search with index, strategy = auto';
SELECT replaceRegexpAll(trimLeft(explain), '__set_Int32_\\d+_\\d+', '__set_Int32_XXX')
FROM (
    EXPLAIN actions = 1
    SELECT key
    FROM tab
    WHERE key IN (0, 30, 60, 90)
    ORDER BY cosineDistance(vec1, arrayMap(i -> randCanonical(i), range(16)))
    LIMIT 1
    SETTINGS vector_search_filter_strategy = 'auto'
)
WHERE trimLeft(explain) LIKE 'Prewhere filter column: %'
   OR trimLeft(explain) LIKE 'Filter column: %';

SELECT '-- Search with index, strategy = prefilter';
SELECT replaceRegexpAll(trimLeft(explain), '__set_Int32_\\d+_\\d+', '__set_Int32_XXX')
FROM (
    EXPLAIN actions = 1
    SELECT key
    FROM tab
    WHERE key IN (0, 30, 60, 90)
    ORDER BY cosineDistance(vec1, arrayMap(i -> randCanonical(i), range(16)))
    LIMIT 1
    SETTINGS vector_search_filter_strategy = 'prefilter'
)
WHERE trimLeft(explain) LIKE 'Prewhere filter column: %'
   OR trimLeft(explain) LIKE 'Filter column: %';

SELECT '-- Search with index, strategy = postfilter';
SELECT replaceRegexpAll(trimLeft(explain), '__set_Int32_\\d+_\\d+', '__set_Int32_XXX')
FROM (
    EXPLAIN actions = 1
    SELECT key
    FROM tab
    WHERE key IN (0, 30, 60, 90)
    ORDER BY cosineDistance(vec1, arrayMap(i -> randCanonical(i), range(16)))
    LIMIT 1
    SETTINGS vector_search_filter_strategy = 'postfilter'
)
WHERE trimLeft(explain) LIKE 'Prewhere filter column: %'
   OR trimLeft(explain) LIKE 'Filter column: %';

SELECT '-----';

-- Test vector search on nonindexed column (vec2) with different filter strategies
--
-- Regardless of filter strategy, prewhere (brute force) distance calculation is expected

SELECT '-- Search without index, strategy = auto';
SELECT replaceRegexpAll(trimLeft(explain), '__set_Int32_\\d+_\\d+', '__set_Int32_XXX')
FROM (
    EXPLAIN actions = 1
    SELECT key
    FROM tab
    WHERE key IN (0, 30, 60, 90)
    ORDER BY cosineDistance(vec2, arrayMap(i -> randCanonical(i), range(32)))
    LIMIT 1
    SETTINGS vector_search_filter_strategy = 'auto'
)
WHERE trimLeft(explain) LIKE 'Prewhere filter column: %'
   OR trimLeft(explain) LIKE 'Filter column: %';

SELECT '-- Search without index, strategy = prefilter';
SELECT replaceRegexpAll(trimLeft(explain), '__set_Int32_\\d+_\\d+', '__set_Int32_XXX')
FROM (
    EXPLAIN actions = 1
    SELECT key
    FROM tab
    WHERE key IN (0, 30, 60, 90)
    ORDER BY cosineDistance(vec2, arrayMap(i -> randCanonical(i), range(32)))
    LIMIT 1
    SETTINGS vector_search_filter_strategy = 'prefilter'
)
WHERE trimLeft(explain) LIKE 'Prewhere filter column: %'
   OR trimLeft(explain) LIKE 'Filter column: %';

SELECT '-- Search without index, strategy = postfilter';
SELECT replaceRegexpAll(trimLeft(explain), '__set_Int32_\\d+_\\d+', '__set_Int32_XXX')
FROM (
    EXPLAIN actions = 1
    SELECT key
    FROM tab
    WHERE key IN (0, 30, 60, 90)
    ORDER BY cosineDistance(vec2, arrayMap(i -> randCanonical(i), range(32)))
    LIMIT 1
    SETTINGS vector_search_filter_strategy = 'postfilter'
)
WHERE trimLeft(explain) LIKE 'Prewhere filter column: %'
   OR trimLeft(explain) LIKE 'Filter column: %';

DROP TABLE tab;
