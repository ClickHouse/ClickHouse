-- Tags: no-fasttest, no-ordinary-database, no-parallel-replicas
-- no-parallel-replicas: EXPLAIN returns a different plan, this is expected behavior

SET allow_experimental_vector_similarity_index=1;
SET enable_analyzer = 1; -- analyzer vs. non-analyzer produce slightly different EXPLAIN

-- Test for issue #77978

DROP TABLE IF EXISTS tab;

CREATE TABLE tab(id Int32, vec1 Array(Float32), vec2 Array(Float32), INDEX idx vec1 TYPE vector_similarity('hnsw', 'L2Distance')) ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (0, [1.0, 0.0], [1.0, 0.0]), (1, [1.1, 0.0], [1.1, 0.0]), (2, [1.2, 0.0], [1.2, 0.0]), (3, [1.3, 0.0], [1.3, 0.0]), (4, [1.4, 0.0], [1.4, 0,0]), (5, [1.5, 0.0], [1.5, 0.0]), (6, [0.0, 2.0], [0.0, 2.0]), (7, [0.0, 2.1], [0.0, 2.1]), (8, [0.0, 2.2], [0.0, 2.2]), (9, [0.0, 2.3], [0.0, 2.3]), (10, [0.0, 2.4], [0.0, 2.4]), (11, [0.0, 2.5], [0.0, 2.5]);

SELECT 'Searches on vec1 should use the vector index';
EXPLAIN indexes=1 WITH [0.0, 2.0] AS reference_vec SELECT id FROM tab ORDER BY L2Distance(vec1, reference_vec) LIMIT 3;

SELECT 'Searches on vec2 should be brute force';
EXPLAIN indexes=1 WITH [0.0, 2.0] AS reference_vec SELECT id FROM tab ORDER BY L2Distance(vec2, reference_vec) LIMIT 3;
