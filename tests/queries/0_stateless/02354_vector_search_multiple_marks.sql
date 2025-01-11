-- Tags: no-fasttest, no-ordinary-database

-- Tests correctness of vector similarity index with > 1 mark

SET allow_experimental_vector_similarity_index = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance')) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192;
INSERT INTO tab SELECT number, [toFloat32(number), 0.0] from numbers(10000);

WITH [1.0, 0.0] AS reference_vec
SELECT id, vec, L2Distance(vec, reference_vec)
FROM tab
ORDER BY L2Distance(vec, reference_vec)
LIMIT 1;

WITH [9000.0, 0.0] AS reference_vec
SELECT id, vec, L2Distance(vec, reference_vec)
FROM tab
ORDER BY L2Distance(vec, reference_vec)
LIMIT 1;

DROP TABLE tab;
