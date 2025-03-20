-- Tags: no-fasttest, no-ordinary-database

set allow_experimental_vector_similarity_index=1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab(id Int32, vec1 Array(Float32), vec2 Array(Float32), INDEX idx vec1 TYPE vector_similarity('hnsw', 'L2Distance')) ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (0, [1.0, 0.0], [1.0, 0.0]), (1, [1.1, 0.0], [1.1, 0.0]), (2, [1.2, 0.0], [1.2, 0.0]), (3, [1.3, 0.0], [1.3, 0.0]), (4, [1.4, 0.0], [1.4, 0,0]), (5, [1.5, 0.0], [1.5, 0.0]), (6, [0.0, 2.0], [0.0, 2.0]), (7, [0.0, 2.1], [0.0, 2.1]), (8, [0.0, 2.2], [0.0, 2.2]), (9, [0.0, 2.3], [0.0, 2.3]), (10, [0.0, 2.4], [0.0, 2.4]), (11, [0.0, 2.5], [0.0, 2.5]);

-- should use vector index
explain indexes=1 WITH [0.0, 2.0] AS reference_vec SELECT id FROM tab ORDER BY L2Distance(vec1, reference_vec) LIMIT 3;

-- should not use vector index as column is vec2
explain indexes=1 WITH [0.0, 2.0] AS reference_vec SELECT id FROM tab ORDER BY L2Distance(vec2, reference_vec) LIMIT 3;
