-- Tags: no-fasttest, no-ordinary-database

-- Test for vector search optimization

SET allow_experimental_vector_similarity_index = 1;
SET enable_analyzer = 1;
SET parallel_replicas_local_plan = 1; -- this setting is randomized, set it explicitly to force local plan for parallel replicas

DROP TABLE IF EXISTS tab;

CREATE TABLE tab(id Int32, vec Array(Float32), attr1 Int32, INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2)) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 2;
INSERT INTO tab VALUES (0, [1.0, 0.0], 50), (1, [1.1, 0.0], 50), (2, [1.2, 0.0], 50), (3, [1.3, 0.0], 50), (4, [1.4, 0.0], 50), (5, [0.0, 2.0], 50), (6, [0.0, 2.1], 50), (7, [0.0, 2.2], 50), (8, [0.0, 2.3], 50), (9, [0.0, 2.4], 50);

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

-- Test vector search optimization and presence of other predicates
-- Output will be 0,1,2
WITH [1.0, 0.0] AS reference_vec
SELECT id
FROM tab
WHERE id <= 3
ORDER BY L2Distance(vec, reference_vec)
LIMIT 3
SETTINGS vector_search_with_rescoring = 0;

-- No rows will be output because 3 nearest neighbours have id 0,1,2
WITH [1.0, 0.0] AS reference_vec
SELECT id, attr1
FROM tab
WHERE id > 3
ORDER BY L2Distance(vec, reference_vec)
LIMIT 3
SETTINGS vector_search_with_rescoring = 0;

-- Increase the multiplier so that we get neighbours for above query
-- Output will be 4,5,6
WITH [1.0, 0.0] AS reference_vec
SELECT id, attr1
FROM tab
WHERE id > 3
ORDER BY L2Distance(vec, reference_vec)
LIMIT 3
SETTINGS vector_search_with_rescoring = 0, vector_search_postfilter_multiplier=10;

-- Predicate on non-PK attribute
-- Output will be 0,1,2,3,4
WITH [1.0, 0.0] AS reference_vec
SELECT id, attr1
FROM tab
WHERE attr1 >= 50
ORDER BY L2Distance(vec, reference_vec)
LIMIT 5
SETTINGS vector_search_with_rescoring = 0;
