-- Tags: no-fasttest, no-ordinary-database

-- Test for vector search optimization

SET allow_experimental_vector_similarity_index = 1;
SET enable_analyzer = 1;
SET parallel_replicas_local_plan = 1; -- this setting is randomized, set it explicitly to force local plan for parallel replicas

DROP TABLE IF EXISTS tab;

CREATE TABLE tab(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2)) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 2;
INSERT INTO tab VALUES (0, [1.0, 0.0]), (1, [1.1, 0.0]), (2, [1.2, 0.0]), (3, [1.3, 0.0]), (4, [1.4, 0.0]), (5, [0.0, 2.0]), (6, [0.0, 2.1]), (7, [0.0, 2.2]), (8, [0.0, 2.3]), (9, [0.0, 2.4]);

SELECT 'Test "SELECT id" without and with rescoring';

WITH [0.0, 2.0] AS reference_vec
SELECT id
FROM tab
ORDER BY L2Distance(vec, reference_vec)
LIMIT 3
SETTINGS vector_search_with_rescoring = 0;

-- Expect column '_distance' in EXPLAIN. Column 'vec' is not expected for ReadFromMergeTree.
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN header = 1
    WITH [0.0, 2.0] AS reference_vec
    SELECT id
    FROM tab
    ORDER BY L2Distance(vec, reference_vec)
    LIMIT 3
    SETTINGS vector_search_with_rescoring = 0)
WHERE (explain LIKE '%_distance%' OR explain LIKE '%vec%Array%') AND explain NOT LIKE '%L2Distance%';

WITH [0.0, 2.0] AS reference_vec
SELECT id
FROM tab
ORDER BY L2Distance(vec, reference_vec)
LIMIT 3
SETTINGS vector_search_with_rescoring = 1;

-- Don't expect column '_distance' in EXPLAIN.
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN header = 1
    WITH [0.0, 2.0] AS reference_vec
    SELECT id
    FROM tab
    ORDER BY L2Distance(vec, reference_vec)
    LIMIT 3
    SETTINGS vector_search_with_rescoring = 1)
WHERE (explain LIKE '%_distance%');

SELECT 'Test "SELECT id, vec" without and with rescoring';
-- In this case, the optimization is not triggered even if vector_search_with_rescoring = 0.

WITH [0.0, 2.0] AS reference_vec
SELECT id, vec
FROM tab
ORDER BY L2Distance(vec, reference_vec)
LIMIT 3
SETTINGS vector_search_with_rescoring = 0;

-- Don't expect column '_distance' in EXPLAIN.
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN header = 1
    WITH [0.0, 2.0] AS reference_vec
    SELECT id, vec
    FROM tab
    ORDER BY L2Distance(vec, reference_vec)
    LIMIT 3
    SETTINGS vector_search_with_rescoring = 0)
WHERE (explain LIKE '%_distance%');

WITH [0.0, 2.0] AS reference_vec
SELECT id, vec
FROM tab
ORDER BY L2Distance(vec, reference_vec)
LIMIT 3
SETTINGS vector_search_with_rescoring = 1;

-- Don't expect column '_distance' in EXPLAIN.
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN header = 1
    WITH [0.0, 2.0] AS reference_vec
    SELECT id, vec
    FROM tab
    ORDER BY L2Distance(vec, reference_vec)
    LIMIT 3
    SETTINGS vector_search_with_rescoring = 1)
WHERE (explain LIKE '%_distance%');
