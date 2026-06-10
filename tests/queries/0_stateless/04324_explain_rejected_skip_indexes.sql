-- Tags: no-fasttest, no-ordinary-database

-- Tests that EXPLAIN indexes=1 shows rejected (not applied) skip indexes with reasons.

SET enable_analyzer = 1;
SET parallel_replicas_local_plan = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id Int32,
    vec Array(Float32),
    attr1 String,
    INDEX idx_vec vec TYPE vector_similarity('hnsw', 'L2Distance', 2) GRANULARITY 10000,
    INDEX idx_bloom attr1 TYPE bloom_filter() GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 3;

INSERT INTO tab VALUES (1, [1.0, 0.0], 'a'), (2, [1.1, 0.0], 'b'), (3, [1.2, 0.0], 'c');

SELECT 'Full EXPLAIN indexes output with rejected indexes';
EXPLAIN indexes = 1
    SELECT id FROM tab WHERE id > 0;

SELECT 'Both skip indexes rejected when query does not match either';
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT id FROM tab WHERE id > 0
)
WHERE explain LIKE '%Name:%' OR explain LIKE '%Status:%' OR explain LIKE '%Reason:%';

SELECT 'vector_similarity applied, bloom_filter rejected (no predicate)';
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT id FROM tab ORDER BY L2Distance(vec, [0.0, 0.0]) LIMIT 3
)
WHERE explain LIKE '%Name:%' OR explain LIKE '%Status:%' OR explain LIKE '%Reason:%';

SELECT 'bloom_filter applied, vector_similarity rejected (no ORDER BY LIMIT)';
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT id FROM tab WHERE attr1 = 'a'
)
WHERE explain LIKE '%Name:%' OR explain LIKE '%Status:%' OR explain LIKE '%Reason:%';

DROP TABLE tab;
