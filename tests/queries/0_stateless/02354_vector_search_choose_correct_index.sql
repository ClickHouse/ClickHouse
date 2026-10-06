-- Tags: no-fasttest, no-ordinary-database

SET parallel_replicas_local_plan = 1; -- this setting is randomized, set it explicitly to have local plan for parallel replicas

-- Test for issue #77978

DROP TABLE IF EXISTS tab;

CREATE TABLE tab(id Int32, vec1 Array(Float32), vec2 Array(Float32), INDEX idx vec1 TYPE vector_similarity('hnsw', 'L2Distance', 2)) ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (0, [1.0, 0.0], [1.0, 0.0]), (1, [1.1, 0.0], [1.1, 0.0]), (2, [1.2, 0.0], [1.2, 0.0]), (3, [1.3, 0.0], [1.3, 0.0]), (4, [1.4, 0.0], [1.4, 0,0]), (5, [1.5, 0.0], [1.5, 0.0]), (6, [0.0, 2.0], [0.0, 2.0]), (7, [0.0, 2.1], [0.0, 2.1]), (8, [0.0, 2.2], [0.0, 2.2]), (9, [0.0, 2.3], [0.0, 2.3]), (10, [0.0, 2.4], [0.0, 2.4]), (11, [0.0, 2.5], [0.0, 2.5]);

SELECT 'Searches on vec1 should use the vector index';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1 WITH [0.0, 2.0] AS reference_vec SELECT id FROM tab ORDER BY L2Distance(vec1, reference_vec) LIMIT 3
)
WHERE explain ILIKE '%Skip%' OR explain ILIKE '%Name: idx%' OR explain ILIKE '%vector_similarity%';

SELECT 'Searches on vec2 should be brute force';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1 WITH [0.0, 2.0] AS reference_vec SELECT id FROM tab ORDER BY L2Distance(vec2, reference_vec) LIMIT 3
)
WHERE explain ILIKE '%Skip%' OR explain ILIKE '%Name: idx%' OR explain ILIKE '%vector_similarity%';

DROP TABLE tab;
