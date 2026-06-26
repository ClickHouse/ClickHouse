-- Tags: no-fasttest, no-ordinary-database

-- Tests 2nd skip index and use_skip_indexes_on_data_read=1/0

SET enable_analyzer = 1;
SET parallel_replicas_local_plan = 1; -- this setting is randomized, set it explicitly to have local plan for parallel replicas

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id Int32,
    date Date,
    attr1 Int32,
    attr2 Int32,
    vec Array(Float32),
    INDEX idx_attr1 attr1 TYPE minmax,
    INDEX idx_vec vec TYPE vector_similarity('hnsw', 'L2Distance', 2) GRANULARITY 10000
)
ENGINE = MergeTree
PARTITION BY date
ORDER BY id
SETTINGS index_granularity = 3;

INSERT INTO tab VALUES
  (1, '2025-01-01', 101, 1001, [1.0, 0.0]),
  (2, '2025-01-01', 102, 1002, [1.1, 0.0]),
  (3, '2025-01-01', 103, 1003, [1.2, 0.0]),
  (4, '2025-01-02', 104, 1003, [1.3, 0.0]),
  (5, '2025-01-02', 105, 1004, [1.4, 0.0]),
  (6, '2025-01-02', 106, 1005, [1.5, 0.0]),
  (7, '2025-01-03', 107, 1005, [1.6, 0.0]),
  (8, '2025-01-03', 108, 1006, [1.7, 0.0]),
  (9, '2025-01-03', 109, 1007, [1.8, 0.0]),
  (10, '2025-01-03', 110, 1008, [1.9, 0.0]),
  (11, '2025-01-03', 111, 1009, [2.0, 0.0]),
  (12, '2025-01-03', 112, 1010, [2.1, 0.0]);

SELECT '-- Additional WHERE clause on skip index column. Expect vector index usage + skip index usage';
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT id
    FROM tab
    WHERE attr1 > 100
    ORDER BY L2Distance(vec, [1.0, 1.0])
    LIMIT 2
    SETTINGS vector_search_filter_strategy = 'postfilter'
)
WHERE explain LIKE '%vector_similarity%' OR explain LIKE '%minmax%';

SET use_query_condition_cache = 1;

SELECT '-- Run the query with use_skip_indexes_on_data_read=0/1 to verify';

SELECT id
FROM tab
WHERE attr1 > 100
ORDER BY L2Distance(vec, [1.0, 1.0])
LIMIT 2
SETTINGS vector_search_filter_strategy = 'postfilter', use_skip_indexes_on_data_read = 1;

SELECT id
FROM tab
WHERE attr1 > 100
ORDER BY L2Distance(vec, [1.0, 1.0])
LIMIT 2
SETTINGS vector_search_filter_strategy = 'postfilter', use_skip_indexes_on_data_read = 0;

SELECT '-- Make sure that query condition cache was not updated by the vector search query';

SELECT count(*)
FROM tab
WHERE attr1 > 100;

DROP TABLE tab;
