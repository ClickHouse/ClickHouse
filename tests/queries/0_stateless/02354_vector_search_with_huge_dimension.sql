-- Tags: no-fasttest, no-ordinary-database

-- Tests vector search over vectors with a huge dimension (32k)

SET parallel_replicas_local_plan = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab(
    id Int32,
    attr1 Int32,
    vec Array(Float32),
    INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 32768))
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 2;

INSERT INTO tab
    SELECT number,
           number * 100,
           CAST(arrayWithConstant(32768, number / 10) as Array(Float32))
    FROM numbers(10);

SELECT '-- Plan must contain vector index usage';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT id
    FROM tab
    ORDER BY L2Distance(vec, arrayWithConstant(32768, 0.2))
    LIMIT 3
)
WHERE explain ILIKE '%Skip%' OR explain ILIKE '%Name: idx%' OR explain ILIKE '%vector_similarity%';

SELECT '-- Run vector search';
-- Nearest vectors to [0.9,0.9...,0.9] are [0.9,...], [0.8,...], [0.7,...]
SELECT id
FROM tab
ORDER BY L2Distance(vec, arrayWithConstant(32768, 0.9))
LIMIT 3;

DROP TABLE tab;
