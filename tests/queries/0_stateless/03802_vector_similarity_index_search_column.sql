-- Tags: no-fasttest

SET enable_analyzer = 1,
    parallel_replicas_local_plan = 1;

DROP TABLE IF EXISTS 03802_data;

CREATE TABLE 03802_data (
    key Int32,
    val String,
    vec_16 Array(BFloat16),
    vec_32 Array(BFloat16),
    INDEX idx_vec_16 vec_16 TYPE vector_similarity('hnsw', 'cosineDistance', 16)
)
ENGINE = MergeTree ORDER BY key
SETTINGS index_granularity = 3;

INSERT INTO 03802_data
SELECT
    number,
    'val-' || number,
    arrayMap(i -> randCanonical(i), range(16)),
    arrayMap(i -> randCanonical(i), range(32))
FROM numbers(100);

SELECT '-- Search over index, strategy = auto';
SELECT replaceRegexpAll(trimLeft(explain), '__set_Int32_\\d+_\\d+', '__set_Int32_XXX')
FROM (
    EXPLAIN actions = 1
    SELECT key, val
    FROM 03802_data
    WHERE key IN (0, 30, 60, 90)
    ORDER BY cosineDistance(vec_16, arrayMap(i -> randCanonical(i), range(16)))
    LIMIT 1
    SETTINGS vector_search_filter_strategy = 'auto'
)
WHERE trimLeft(explain) LIKE 'Prewhere filter column: %'
   OR trimLeft(explain) LIKE 'Filter column: %';

SELECT '-- Search over index, strategy = prefilter';
SELECT replaceRegexpAll(trimLeft(explain), '__set_Int32_\\d+_\\d+', '__set_Int32_XXX')
FROM (
    EXPLAIN actions = 1
    SELECT key, val
    FROM 03802_data
    WHERE key IN (0, 30, 60, 90)
    ORDER BY cosineDistance(vec_16, arrayMap(i -> randCanonical(i), range(16)))
    LIMIT 1
    SETTINGS vector_search_filter_strategy = 'prefilter'
)
WHERE trimLeft(explain) LIKE 'Prewhere filter column: %'
   OR trimLeft(explain) LIKE 'Filter column: %';

SELECT '-- Search over index, strategy = postfilter';
SELECT replaceRegexpAll(trimLeft(explain), '__set_Int32_\\d+_\\d+', '__set_Int32_XXX')
FROM (
    EXPLAIN actions = 1
    SELECT key, val
    FROM 03802_data
    WHERE key IN (0, 30, 60, 90)
    ORDER BY cosineDistance(vec_16, arrayMap(i -> randCanonical(i), range(16)))
    LIMIT 1
    SETTINGS vector_search_filter_strategy = 'postfilter'
)
WHERE trimLeft(explain) LIKE 'Prewhere filter column: %'
   OR trimLeft(explain) LIKE 'Filter column: %';

SELECT '';

SELECT '-- Search without index, strategy = auto';
SELECT replaceRegexpAll(trimLeft(explain), '__set_Int32_\\d+_\\d+', '__set_Int32_XXX')
FROM (
    EXPLAIN actions = 1
    SELECT key, val
    FROM 03802_data
    WHERE key IN (0, 30, 60, 90)
    ORDER BY cosineDistance(vec_32, arrayMap(i -> randCanonical(i), range(32)))
    LIMIT 1
    SETTINGS vector_search_filter_strategy = 'auto'
)
WHERE trimLeft(explain) LIKE 'Prewhere filter column: %'
   OR trimLeft(explain) LIKE 'Filter column: %';

SELECT '-- Search without index, strategy = prefilter';
SELECT replaceRegexpAll(trimLeft(explain), '__set_Int32_\\d+_\\d+', '__set_Int32_XXX')
FROM (
    EXPLAIN actions = 1
    SELECT key, val
    FROM 03802_data
    WHERE key IN (0, 30, 60, 90)
    ORDER BY cosineDistance(vec_32, arrayMap(i -> randCanonical(i), range(32)))
    LIMIT 1
    SETTINGS vector_search_filter_strategy = 'prefilter'
)
WHERE trimLeft(explain) LIKE 'Prewhere filter column: %'
   OR trimLeft(explain) LIKE 'Filter column: %';

SELECT '-- Search without index, strategy = postfilter';
SELECT replaceRegexpAll(trimLeft(explain), '__set_Int32_\\d+_\\d+', '__set_Int32_XXX')
FROM (
    EXPLAIN actions = 1
    SELECT key, val
    FROM 03802_data
    WHERE key IN (0, 30, 60, 90)
    ORDER BY cosineDistance(vec_32, arrayMap(i -> randCanonical(i), range(32)))
    LIMIT 1
    SETTINGS vector_search_filter_strategy = 'postfilter'
)
WHERE trimLeft(explain) LIKE 'Prewhere filter column: %'
   OR trimLeft(explain) LIKE 'Filter column: %';

DROP TABLE 03802_data;
