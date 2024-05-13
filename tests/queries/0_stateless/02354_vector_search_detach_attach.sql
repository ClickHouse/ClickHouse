-- Tags: no-fasttest, no-ordinary-database

-- Tests that vector similarity indexes can be detached/attached.

SET allow_experimental_usearch_index = 1;

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (id Int32, vec Array(Float32), PRIMARY KEY id, INDEX vec_idx(vec) TYPE vector_similarity('hnsw', 'L2Distance'));
INSERT INTO tab VALUES (0, [1.0, 0.0]), (1, [1.1, 0.0]), (2, [1.2, 0.0]), (3, [1.3, 0.0]), (4, [1.4, 0.0]), (5, [1.5, 0.0]), (6, [0.0, 2.0]), (7, [0.0, 2.1]), (8, [0.0, 2.2]), (9, [0.0, 2.3]), (10, [0.0, 2.4]), (11, [0.0, 2.5]);

DETACH TABLE tab SYNC;
ATTACH TABLE tab;

WITH [0.0, 2.0] AS reference_vec
SELECT id, vec, L2Distance(vec, reference_vec)
FROM tab
ORDER BY L2Distance(vec, reference_vec)
LIMIT 3;

DROP TABLE tab;
