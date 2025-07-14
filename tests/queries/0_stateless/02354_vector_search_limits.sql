-- Tags: no-fasttest, no-ordinary-database

-- Tests for various limits in vector search

SET enable_vector_similarity_index = 1;
SET parallel_replicas_local_plan = 1;

DROP TABLE IF EXISTS tab;

SELECT 'Verify vector column & index with dimension=32KB';

CREATE TABLE tab(id Int32, attr1 Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 32768)) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity=2;

INSERT INTO tab SELECT number, number * 100, CAST(arrayWithConstant(32768, number / 10) as Array(Float32)) FROM numbers(10);

SELECT 'Search should use vector index';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1 SELECT id FROM tab ORDER BY L2Distance(vec, arrayWithConstant(32768, 0.2)) LIMIT 3
)
WHERE explain ILIKE '%Skip%' OR explain ILIKE '%Name: idx%' OR explain ILIKE '%vector_similarity%';

-- Nearest vectors to [0.9,0.9...,0.9] are [0.9,...], [0.8,...], [0.7,...]
SELECT id
FROM tab
ORDER BY L2Distance(vec, arrayWithConstant(32768, 0.9))
LIMIT 3;

SELECT 'vector_search_postfilter_multiplier greater than 1000 should error';

SELECT id
FROM tab
WHERE attr1 > 200
ORDER BY L2Distance(vec, arrayWithConstant(32768, 0.9))
LIMIT 3
SETTINGS vector_search_postfilter_multiplier=1001; -- { serverError INVALID_SETTING_VALUE }

SELECT id
FROM tab
WHERE attr1 > 200
ORDER BY L2Distance(vec, arrayWithConstant(32768, 0.9))
LIMIT 3
SETTINGS vector_search_postfilter_multiplier=1000;

SELECT 'Next query should not use index as limit for ANN is low';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1 SELECT id FROM tab ORDER BY L2Distance(vec, arrayWithConstant(32768, 0.2)) LIMIT 6 SETTINGS max_limit_for_vector_search_queries=3
)
WHERE explain ILIKE '%Skip%' OR explain ILIKE '%Name: idx%' OR explain ILIKE '%vector_similarity%';
