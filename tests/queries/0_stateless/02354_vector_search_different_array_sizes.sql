-- Tags: no-fasttest, no-ordinary-database

SET allow_experimental_vector_similarity_index = 1;
SET enable_analyzer = 1; -- 0 vs. 1 produce slightly different error codes, make it future-proof

DROP TABLE IF EXISTS tab;

CREATE TABLE tab(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance')) ENGINE = MergeTree ORDER BY id;

-- Vector similarity indexes reject INSERTs of Arrays with different sizes
INSERT INTO tab values (0, [2.2, 2.3]) (1, [3.1, 3.2, 3.3]); -- { serverError INCORRECT_DATA }

-- It is possible to create parts with different Array vector sizes but there will be an error at query time
SYSTEM STOP MERGES tab;
INSERT INTO tab values (0, [2.2, 2.3]) (1, [3.1, 3.2]);
INSERT INTO tab values (2, [2.2, 2.3, 2.4]) (3, [3.1, 3.2, 3.3]);

WITH [0.0, 2.0] AS reference_vec
SELECT id, vec, L2Distance(vec, reference_vec)
FROM tab
ORDER BY L2Distance(vec, reference_vec)
LIMIT 3; -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }

DROP TABLE tab;
