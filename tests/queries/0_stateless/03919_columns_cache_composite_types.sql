-- Test columns cache with complex composite types and partial reads
-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-replicated-database

SET use_columns_cache = 1;
SET enable_reads_from_columns_cache = 1;
SET enable_writes_to_columns_cache = 1;
SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS t_cache_composite;

-- Create table with various composite types
CREATE TABLE t_cache_composite (
    id UInt64,
    -- Nested arrays with LowCardinality
    tags Array(LowCardinality(Nullable(String))),
    -- Array of Tuples
    metrics Array(Tuple(name String, value Int64)),
    -- Map type
    attributes Map(String, Int64),
    -- LowCardinality Nullable
    category LowCardinality(Nullable(String)),
    -- Nested arrays
    matrix Array(Array(Int64)),
    -- Complex tuple
    metadata Tuple(
        created DateTime,
        author String,
        flags Array(String)
    ),
    -- Array of Maps
    configs Array(Map(String, String)),
    -- Tuple with nullable fields and arrays
    location Tuple(
        city Nullable(String),
        country LowCardinality(String),
        coordinates Array(Float64)
    )
) ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1000;

-- Insert test data
INSERT INTO t_cache_composite
SELECT
    number AS id,
    if(number % 3 = 0, [NULL, 'tag1', 'tag2'], ['tag' || toString(number % 10), NULL]) AS tags,
    [('cpu', number * 2), ('memory', number * 3), ('disk', number * 5)] AS metrics,
    map('key1', number, 'key2', number * 2, 'key3', number * 3) AS attributes,
    if(number % 5 = 0, NULL, 'category_' || toString(number % 7)) AS category,
    [[number, number + 1], [number * 2, number * 2 + 1]] AS matrix,
    tuple(
        now() - INTERVAL number SECOND,
        'user_' || toString(number % 100),
        ['flag_' || toString(number % 3), 'flag_' || toString(number % 5)]
    ) AS metadata,
    [map('config1', 'value' || toString(number)), map('config2', 'value' || toString(number * 2))] AS configs,
    tuple(
        if(number % 4 = 0, NULL, 'city_' || toString(number % 20)),
        'country_' || toString(number % 10),
        [toFloat64(number) / 100, toFloat64(number + 1) / 100]
    ) AS location
FROM numbers(10000);

SYSTEM DROP COLUMNS CACHE;

-- Test 1: Array(LowCardinality(Nullable(String))) with cache
SELECT count(), sum(length(tags)) FROM t_cache_composite;
SELECT count(), sum(length(tags)) FROM t_cache_composite;
SELECT count(), sum(length(tags)) FROM t_cache_composite;

-- Test 2: Array(Tuple) operations
SELECT sum(metrics[1].value), sum(metrics[2].value) FROM t_cache_composite;
SELECT sum(metrics[1].value), sum(metrics[2].value) FROM t_cache_composite;

-- Test 3: Map operations
SELECT count(), sum(attributes['key1']), sum(attributes['key2']) FROM t_cache_composite;
SELECT count(), sum(attributes['key1']), sum(attributes['key2']) FROM t_cache_composite;

-- Test 4: LowCardinality(Nullable(String))
SELECT category, count(*) FROM t_cache_composite WHERE category IS NOT NULL GROUP BY category ORDER BY category LIMIT 5;
SELECT category, count(*) FROM t_cache_composite WHERE category IS NOT NULL GROUP BY category ORDER BY category LIMIT 5;

-- Test 5: Array(Array) nested arrays
SELECT sum(matrix[1][1]), sum(matrix[2][2]) FROM t_cache_composite;
SELECT sum(matrix[1][1]), sum(matrix[2][2]) FROM t_cache_composite;

-- Test 6: Complex Tuple access
SELECT count(), sum(length(metadata.flags)) FROM t_cache_composite WHERE id % 100 = 0;
SELECT count(), sum(length(metadata.flags)) FROM t_cache_composite WHERE id % 100 = 0;

-- Test 7: Array(Map) operations
SELECT count() FROM t_cache_composite WHERE configs[1]['config1'] <> '';
SELECT count() FROM t_cache_composite WHERE configs[1]['config1'] <> '';

-- Test 8: Tuple with Nullable and Array
SELECT count() FROM t_cache_composite WHERE location.city IS NOT NULL;
SELECT count() FROM t_cache_composite WHERE location.city IS NOT NULL;

-- Test 9: Multiple composite columns together
SELECT count(), sum(length(tags)), sum(attributes['key1']), sum(matrix[1][1])
FROM t_cache_composite
WHERE id < 5000;

SELECT count(), sum(length(tags)), sum(attributes['key1']), sum(matrix[1][1])
FROM t_cache_composite
WHERE id < 5000;

-- Test 10: Partial range reads
SYSTEM DROP COLUMNS CACHE;
SELECT sum(id) FROM t_cache_composite WHERE id BETWEEN 1000 AND 2000;
SELECT sum(id) FROM t_cache_composite WHERE id BETWEEN 3000 AND 4000;
SELECT sum(id) FROM t_cache_composite WHERE id BETWEEN 1000 AND 2000;

DROP TABLE t_cache_composite;

SELECT 'All composite type tests passed';
