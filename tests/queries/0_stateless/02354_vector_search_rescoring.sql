-- Tags: no-fasttest, no-ordinary-database

-- Test for vector search optimization

SET enable_analyzer = 1;
SET allow_experimental_vector_similarity_index=1;
SET parallel_replicas_local_plan=1; -- this setting is randomized, set it explicitly to have local plan for parallel replicas

DROP TABLE IF EXISTS tab;

CREATE TABLE tab(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2)) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 2;
INSERT INTO tab VALUES (0, [1.0, 0.0]), (1, [1.1, 0.0]), (2, [1.2, 0.0]), (3, [1.3, 0.0]), (4, [1.4, 0.0]), (5, [0.0, 2.0]), (6, [0.0, 2.1]), (7, [0.0, 2.2]), (8, [0.0, 2.3]), (9, [0.0, 2.4]);

WITH [0.0, 2.0] AS reference_vec
SELECT id
FROM tab
ORDER BY L2Distance(vec, reference_vec)
LIMIT 3
SETTINGS vector_search_with_rescoring = 0;

-- _distance should be seen and "vec" column should not be seen in ReadFromMergeTree
SELECT trimLeft(explain) AS explain FROM (
EXPLAIN header=1
WITH [0.0, 2.0] AS reference_vec
SELECT id
FROM tab
ORDER BY L2Distance(vec, reference_vec)
LIMIT 3
SETTINGS vector_search_with_rescoring = 0
)
WHERE (explain LIKE '%_distance%' OR explain LIKE '%vec%Array%') AND explain NOT LIKE '%L2Distance%';

WITH [0.0, 2.0] AS reference_vec
SELECT id
FROM tab
ORDER BY L2Distance(vec, reference_vec)
LIMIT 3
SETTINGS vector_search_with_rescoring = 1;

-- _distance should not be seen
SELECT trimLeft(explain) AS explain FROM (
EXPLAIN header=1
WITH [0.0, 2.0] AS reference_vec
SELECT id
FROM tab
ORDER BY L2Distance(vec, reference_vec)
LIMIT 3
SETTINGS vector_search_with_rescoring = 1
)
WHERE (explain LIKE '%_distance%');

WITH [0.0, 2.0] AS reference_vec
SELECT id, vec
FROM tab
ORDER BY L2Distance(vec, reference_vec)
LIMIT 3
SETTINGS vector_search_with_rescoring = 0;

-- Explicitly select "vec" column, optimization not possible
-- _distance should not be seen
SELECT trimLeft(explain) AS explain FROM (
EXPLAIN header=1
WITH [0.0, 2.0] AS reference_vec
SELECT id, vec
FROM tab
ORDER BY L2Distance(vec, reference_vec)
LIMIT 3
SETTINGS vector_search_with_rescoring = 0
)
WHERE (explain LIKE '%_distance%');

WITH [0.0, 2.0] AS reference_vec
SELECT id, vec
FROM tab
ORDER BY L2Distance(vec, reference_vec)
LIMIT 3
SETTINGS vector_search_with_rescoring = 1;

-- Explicitly select "vec" column, optimization not possible
-- _distance should not be seen
SELECT trimLeft(explain) AS explain FROM (
EXPLAIN header=1
WITH [0.0, 2.0] AS reference_vec
SELECT id, vec
FROM tab
ORDER BY L2Distance(vec, reference_vec)
LIMIT 3
SETTINGS vector_search_with_rescoring = 1
)
WHERE (explain LIKE '%_distance%');
