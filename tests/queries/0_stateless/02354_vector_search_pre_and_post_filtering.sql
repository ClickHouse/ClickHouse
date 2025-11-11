-- Tags: no-fasttest, no-ordinary-database

-- Tests pre vs. post-filtering for vector search.

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

SELECT 'Test vector_search_filter_strategy = prefilter';

SELECT '-- No additional WHERE clauses present, expect index usage';
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT id
    FROM tab
    ORDER BY L2Distance(vec, [1.0, 1.0])
    LIMIT 2
    SETTINGS vector_search_filter_strategy = 'prefilter'
)
WHERE explain LIKE '%vector_similarity%' OR explain LIKE '%Granules: 3/4%';

SELECT '-- Additional WHERE clauses present, index usage not expected';
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT id
    FROM tab
    WHERE attr2 >= 1006
    ORDER BY L2Distance(vec, [1.0, 1.0])
    LIMIT 2
    SETTINGS vector_search_filter_strategy = 'prefilter'
)
WHERE explain LIKE '%vector_similarity%';

SELECT '-- Additional WHERE clauses present, index usage not expected';
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT id
    FROM tab
    WHERE attr1 <= 105
    ORDER BY L2Distance(vec, [1.0, 1.0])
    LIMIT 2
    SETTINGS vector_search_filter_strategy = 'prefilter'
)
WHERE explain LIKE '%vector_similarity%';

SELECT '-- Additional WHERE clauses present, index usage not expected';
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT id
    FROM tab
    WHERE id <= 6
    ORDER BY L2Distance(vec, [1.0, 1.0])
    LIMIT 2
    SETTINGS vector_search_filter_strategy = 'prefilter'
)
WHERE explain LIKE '%vector_similarity%';

SELECT 'Test vector_search_filter_strategy = postfilter';

SELECT '-- No additional WHERE clauses present, expect index usage';
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT id
    FROM tab
    ORDER BY L2Distance(vec, [1.0, 1.0])
    LIMIT 2
    SETTINGS vector_search_filter_strategy = 'postfilter'
)
WHERE explain LIKE '%vector_similarity%' OR explain LIKE '%Granules: 3/4%';

SELECT '-- Additional WHERE clauses on partition key present (2 full parts selected), expect index usage';
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT id
    FROM tab
    WHERE date <= '2025-01-02'
    ORDER BY L2Distance(vec, [1.0, 1.0])
    LIMIT 2
    SETTINGS vector_search_filter_strategy = 'postfilter'
)
WHERE explain LIKE '%vector_similarity%';

SELECT '-- Additional WHERE clauses on partition key present (2 full parts selected), expect index usage';
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT id
    FROM tab
    WHERE date = '2025-01-03'
    AND attr1 = 110
    ORDER BY L2Distance(vec, [1.0, 1.0])
    LIMIT 2
    SETTINGS vector_search_filter_strategy = 'postfilter'
)
WHERE explain LIKE '%vector_similarity%';

SELECT '-- Additional WHERE clauses present, 2 full parts selected by partition key / 1 part partially selected by PK, index usage not expected';
SELECT id
FROM tab
WHERE date = '2025-01-03' AND id <= 9
ORDER BY L2Distance(vec, [1.0, 1.0])
LIMIT 2
SETTINGS log_comment = '02354_vector_search_post_filter_strategy_query1', vector_search_with_rescoring = 1;

SYSTEM FLUSH LOGS query_log;

SELECT DISTINCT ProfileEvents['USearchSearchCount']
FROM system.query_log
WHERE log_comment = '02354_vector_search_post_filter_strategy_query1'
AND current_database = currentDatabase()
AND type = 'QueryFinish';

SELECT 'The first 3 neighbours returned by vector index dont pass the attr2 >= 1008 filter. Hence no rows returned by the query...';
SELECT id
FROM tab
WHERE date = '2025-01-03' AND attr2 >= 1008
ORDER BY L2Distance(vec, [1.0, 1.0])
LIMIT 3;

SELECT '... but there are results for the same query with postfilter multiplier = 2.0';
SELECT id
FROM tab
WHERE date = '2025-01-03' AND attr2 >= 1008
ORDER BY L2Distance(vec, [1.0, 1.0])
LIMIT 3
SETTINGS vector_search_index_fetch_multiplier = 2.0;

SELECT '-- Negative parameter values throw an exception';
SELECT id
FROM tab
WHERE date = '2025-01-03' AND attr2 >= 1008
ORDER BY L2Distance(vec, [1.0, 1.0])
LIMIT 3
SETTINGS vector_search_index_fetch_multiplier = -1.0; -- { serverError INVALID_SETTING_VALUE }

SELECT '-- Zero parameter values throw an exception';
SELECT id
FROM tab
WHERE date = '2025-01-03' AND attr2 >= 1008
ORDER BY L2Distance(vec, [1.0, 1.0])
LIMIT 3
SETTINGS vector_search_index_fetch_multiplier = 0.0; -- { serverError INVALID_SETTING_VALUE }

SELECT '-- Too large parameter values throw an exception';
SELECT id
FROM tab
WHERE date = '2025-01-03' AND attr2 >= 1008
ORDER BY L2Distance(vec, [1.0, 1.0])
LIMIT 3
SETTINGS vector_search_index_fetch_multiplier = 1001.0; -- { serverError INVALID_SETTING_VALUE }

DROP TABLE tab;
