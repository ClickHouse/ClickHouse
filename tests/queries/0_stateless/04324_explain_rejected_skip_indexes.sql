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

SELECT 'Both indexes rejected';
EXPLAIN indexes = 1
    SELECT id FROM tab WHERE id > 0;

SELECT 'Both indexes applied';
EXPLAIN indexes = 1
    SELECT id FROM tab WHERE attr1 = 'a' ORDER BY L2Distance(vec, [0.0, 0.0]) LIMIT 3
    SETTINGS vector_search_filter_strategy = 'postfilter';

DROP TABLE tab;
