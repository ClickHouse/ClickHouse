-- Tags: no-fasttest, no-ordinary-database

-- Tests queries utilizing vector similarity indexes with different distance functions.

SET allow_experimental_usearch_index = 1;
SET allow_experimental_analyzer = 0;

-- Not a systematic test, just to check that no bad things happen.

SELECT '-- L2Distance';

DROP TABLE IF EXISTS tab;

CREATE TABLE tab(id Int32, vec Array(Float32), INDEX vec_idx vec TYPE vector_similarity('hnsw', 'L2Distance') GRANULARITY 2) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 3;
INSERT INTO tab VALUES (0, [1.0, 0.0]), (1, [1.1, 0.0]), (2, [1.2, 0.0]), (3, [1.3, 0.0]), (4, [1.4, 0.0]), (5, [0.0, 2.0]), (6, [0.0, 2.1]), (7, [0.0, 2.2]), (8, [0.0, 2.3]), (9, [0.0, 2.4]);

WITH [0.0, 2.0] AS reference_vec
SELECT id, vec, L2Distance(vec, reference_vec)
FROM tab
ORDER BY L2Distance(vec, reference_vec)
LIMIT 3;

EXPLAIN indexes=1
WITH [0.0, 2.0] AS reference_vec
SELECT id, vec, L2Distance(vec, reference_vec)
FROM tab
ORDER BY L2Distance(vec, reference_vec)
LIMIT 3;

-- This alias must also works
EXPLAIN indexes=1
WITH [0.0, 2.0] AS reference_vec
SELECT id, vec, DistanceL2(vec, reference_vec)
FROM tab
ORDER BY DistanceL2(vec, reference_vec)
LIMIT 3;

--- When the query uses an incompatible distance function, the index must not be used
EXPLAIN indexes=1
WITH [0.0, 2.0] AS reference_vec
SELECT id, vec, cosineDistance(vec, reference_vec)
FROM tab
ORDER BY cosineDistance(vec, reference_vec)
LIMIT 3;

DROP TABLE tab;


SELECT '-- cosineDistance';

CREATE TABLE tab(id Int32, vec Array(Float32), INDEX vec_idx vec TYPE vector_similarity('hnsw', 'cosineDistance') GRANULARITY 2) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 3;
INSERT INTO tab VALUES ( , [4.6, 2.3]), (1, [2.0, 3.2]), (2, [4.2, 3.4]), (3, [5.3, 2.9]), (4, [2.4, 5.2]), (5, [5.3, 2.3]), (6, [1.0, 9.3]), (7, [5.5, 4.7]), (8, [6.4, 3.5]), (9, [5.3, 2.5]), (10, [6.4, 3.4]), (11, [6.4, 3.2]);

WITH [0.0, 2.0] AS reference_vec
SELECT id, vec, cosineDistance(vec, reference_vec)
FROM tab
ORDER BY cosineDistance(vec, reference_vec)
LIMIT 3;

EXPLAIN indexes=1
WITH [0.0, 2.0] AS reference_vec
SELECT id, vec, cosineDistance(vec, reference_vec)
FROM tab
ORDER BY cosineDistance(vec, reference_vec)
LIMIT 3;

DROP TABLE tab;

SELECT 'dotProduct';
CREATE TABLE tab(id Int32, vec Array(Float32), INDEX vec_idx vec TYPE vector_similarity('hnsw', 'dotProduct') GRANULARITY 2) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 3;
INSERT INTO tab VALUES (0, [4.6, 2.3]), (1, [2.0, 3.2]), (2, [4.2, 3.4]), (3, [5.3, 2.9]), (4, [2.4, 5.2]), (5, [5.3, 2.3]), (6, [1.0, 9.3]), (7, [5.5, 4.7]), (8, [6.4, 3.5]), (9, [5.3, 2.5]), (10, [6.4, 3.4]), (11, [6.4, 3.2]);

WITH [0.0, 2.0] AS reference_vec
SELECT id, vec, cosineDistance(vec, reference_vec)
FROM tab
ORDER BY cosineDistance(vec, reference_vec)
LIMIT 3;

EXPLAIN indexes=1
WITH [0.0, 2.0] AS reference_vec
SELECT id, vec, dotProduct(vec, reference_vec)
FROM tab
ORDER BY dotProduct(vec, reference_vec)
LIMIT 3;

-- This alias must also work ...
EXPLAIN indexes=1
WITH [0.0, 2.0] AS reference_vec
SELECT id, vec, arrayDotProduct(vec, reference_vec)
FROM tab
ORDER BY arrayDotProduct(vec, reference_vec)
LIMIT 3;

-- ... and this one too
EXPLAIN indexes=1
WITH [0.0, 2.0] AS reference_vec
SELECT id, vec, scalarProduct(vec, reference_vec)
FROM tab
ORDER BY scalarProduct(vec, reference_vec)
LIMIT 3;

DROP TABLE tab;
