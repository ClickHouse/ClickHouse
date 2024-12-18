-- Tags: no-fasttest, no-ordinary-database

-- Tests various simple approximate nearest neighborhood (ANN) queries that utilize vector search indexes.

SET allow_experimental_vector_similarity_index = 1;

SET enable_analyzer = 0;

SELECT '10 rows, index_granularity = 8192, GRANULARITY = 1 million --> 1 granule, 1 indexed block';

DROP TABLE IF EXISTS tab;

CREATE TABLE tab(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance')) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192;
INSERT INTO tab VALUES (0, [1.0, 0.0]), (1, [1.1, 0.0]), (2, [1.2, 0.0]), (3, [1.3, 0.0]), (4, [1.4, 0.0]), (5, [0.0, 2.0]), (6, [0.0, 2.1]), (7, [0.0, 2.2]), (8, [0.0, 2.3]), (9, [0.0, 2.4]);


WITH [0.0, 2.0] AS reference_vec
SELECT id, vec, L2Distance(vec, reference_vec)
FROM tab
ORDER BY L2Distance(vec, reference_vec)
LIMIT 3;

EXPLAIN indexes = 1
WITH [0.0, 2.0] AS reference_vec
SELECT id, vec, L2Distance(vec, reference_vec)
FROM tab
ORDER BY L2Distance(vec, reference_vec)
LIMIT 3;

DROP TABLE tab;


SELECT '12 rows, index_granularity = 3, GRANULARITY = 2 --> 4 granules, 2 indexed block';

CREATE TABLE tab(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance') GRANULARITY 2) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 3;
INSERT INTO tab VALUES (0, [1.0, 0.0]), (1, [1.1, 0.0]), (2, [1.2, 0.0]), (3, [1.3, 0.0]), (4, [1.4, 0.0]), (5, [1.5, 0.0]), (6, [0.0, 2.0]), (7, [0.0, 2.1]), (8, [0.0, 2.2]), (9, [0.0, 2.3]), (10, [0.0, 2.4]), (11, [0.0, 2.5]);

WITH [0.0, 2.0] AS reference_vec
SELECT id, vec, L2Distance(vec, reference_vec)
FROM tab
ORDER BY L2Distance(vec, reference_vec)
LIMIT 3;

EXPLAIN indexes = 1
WITH [0.0, 2.0] AS reference_vec
SELECT id, vec, L2Distance(vec, reference_vec)
FROM tab
ORDER BY L2Distance(vec, reference_vec)
LIMIT 3;

DROP TABLE tab;


SELECT 'Special cases'; -- Not a systematic test, just to check that no bad things happen.

SELECT '-- Non-default metric, hnsw_max_connections_per_layer, hnsw_candidate_list_size_for_construction';
CREATE TABLE tab(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'cosineDistance', 'f32', 42, 99) GRANULARITY 2) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 3;
INSERT INTO tab VALUES (0, [4.6, 2.3]), (1, [2.0, 3.2]), (2, [4.2, 3.4]), (3, [5.3, 2.9]), (4, [2.4, 5.2]), (5, [5.3, 2.3]), (6, [1.0, 9.3]), (7, [5.5, 4.7]), (8, [6.4, 3.5]), (9, [5.3, 2.5]), (10, [6.4, 3.4]), (11, [6.4, 3.2]);

WITH [0.0, 2.0] AS reference_vec
SELECT id, vec, cosineDistance(vec, reference_vec)
FROM tab
ORDER BY cosineDistance(vec, reference_vec)
LIMIT 3;

EXPLAIN indexes = 1
WITH [0.0, 2.0] AS reference_vec
SELECT id, vec, cosineDistance(vec, reference_vec)
FROM tab
ORDER BY cosineDistance(vec, reference_vec)
LIMIT 3;

SELECT '-- Setting "max_limit_for_ann_queries"';
EXPLAIN indexes=1
WITH [0.0, 2.0] as reference_vec
SELECT id, vec, cosineDistance(vec, reference_vec)
FROM tab
ORDER BY cosineDistance(vec, reference_vec)
LIMIT 3
SETTINGS max_limit_for_ann_queries = 2; -- LIMIT 3 > 2 --> don't use the ann index

DROP TABLE tab;

SELECT '-- Test all distance metrics x all quantization';

DROP TABLE IF EXISTS tab_l2_f64;
DROP TABLE IF EXISTS tab_l2_f32;
DROP TABLE IF EXISTS tab_l2_f16;
DROP TABLE IF EXISTS tab_l2_bf16;
DROP TABLE IF EXISTS tab_l2_i8;
DROP TABLE IF EXISTS tab_cos_f64;
DROP TABLE IF EXISTS tab_cos_f32;
DROP TABLE IF EXISTS tab_cos_f16;
DROP TABLE IF EXISTS tab_cos_bf16;
DROP TABLE IF EXISTS tab_cos_i8;

CREATE TABLE tab_l2_f64(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 'f64', 0, 0) GRANULARITY 2) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 3;
CREATE TABLE tab_l2_f32(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 'f32', 0, 0) GRANULARITY 2) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 3;
CREATE TABLE tab_l2_f16(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 'f16', 0, 0) GRANULARITY 2) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 3;
CREATE TABLE tab_l2_bf16(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 'bf16', 0, 0) GRANULARITY 2) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 3;
CREATE TABLE tab_l2_i8(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 'i8', 0, 0) GRANULARITY 2) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 3;
CREATE TABLE tab_cos_f64(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'cosineDistance', 'f64', 0, 0) GRANULARITY 2) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 3;
CREATE TABLE tab_cos_f32(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'cosineDistance', 'f32', 0, 0) GRANULARITY 2) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 3;
CREATE TABLE tab_cos_f16(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'cosineDistance', 'f16', 0, 0) GRANULARITY 2) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 3;
CREATE TABLE tab_cos_bf16(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'cosineDistance', 'bf16', 0, 0) GRANULARITY 2) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 3;
CREATE TABLE tab_cos_i8(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'cosineDistance', 'i8', 0, 0) GRANULARITY 2) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 3;

INSERT INTO tab_l2_f64 VALUES (0, [4.6, 2.3]), (1, [2.0, 3.2]), (2, [4.2, 3.4]), (3, [5.3, 2.9]), (4, [2.4, 5.2]), (5, [5.3, 2.3]), (6, [1.0, 9.3]), (7, [5.5, 4.7]), (8, [6.4, 3.5]), (9, [5.3, 2.5]), (10, [6.4, 3.4]), (11, [6.4, 3.2]);
INSERT INTO tab_l2_f32 VALUES (0, [4.6, 2.3]), (1, [2.0, 3.2]), (2, [4.2, 3.4]), (3, [5.3, 2.9]), (4, [2.4, 5.2]), (5, [5.3, 2.3]), (6, [1.0, 9.3]), (7, [5.5, 4.7]), (8, [6.4, 3.5]), (9, [5.3, 2.5]), (10, [6.4, 3.4]), (11, [6.4, 3.2]);
INSERT INTO tab_l2_f16 VALUES (0, [4.6, 2.3]), (1, [2.0, 3.2]), (2, [4.2, 3.4]), (3, [5.3, 2.9]), (4, [2.4, 5.2]), (5, [5.3, 2.3]), (6, [1.0, 9.3]), (7, [5.5, 4.7]), (8, [6.4, 3.5]), (9, [5.3, 2.5]), (10, [6.4, 3.4]), (11, [6.4, 3.2]);
INSERT INTO tab_l2_bf16 VALUES (0, [4.6, 2.3]), (1, [2.0, 3.2]), (2, [4.2, 3.4]), (3, [5.3, 2.9]), (4, [2.4, 5.2]), (5, [5.3, 2.3]), (6, [1.0, 9.3]), (7, [5.5, 4.7]), (8, [6.4, 3.5]), (9, [5.3, 2.5]), (10, [6.4, 3.4]), (11, [6.4, 3.2]);
INSERT INTO tab_l2_i8 VALUES (0, [4.6, 2.3]), (1, [2.0, 3.2]), (2, [4.2, 3.4]), (3, [5.3, 2.9]), (4, [2.4, 5.2]), (5, [5.3, 2.3]), (6, [1.0, 9.3]), (7, [5.5, 4.7]), (8, [6.4, 3.5]), (9, [5.3, 2.5]), (10, [6.4, 3.4]), (11, [6.4, 3.2]);
INSERT INTO tab_cos_f64 VALUES (0, [4.6, 2.3]), (1, [2.0, 3.2]), (2, [4.2, 3.4]), (3, [5.3, 2.9]), (4, [2.4, 5.2]), (5, [5.3, 2.3]), (6, [1.0, 9.3]), (7, [5.5, 4.7]), (8, [6.4, 3.5]), (9, [5.3, 2.5]), (10, [6.4, 3.4]), (11, [6.4, 3.2]);
INSERT INTO tab_cos_f32 VALUES (0, [4.6, 2.3]), (1, [2.0, 3.2]), (2, [4.2, 3.4]), (3, [5.3, 2.9]), (4, [2.4, 5.2]), (5, [5.3, 2.3]), (6, [1.0, 9.3]), (7, [5.5, 4.7]), (8, [6.4, 3.5]), (9, [5.3, 2.5]), (10, [6.4, 3.4]), (11, [6.4, 3.2]);
INSERT INTO tab_cos_f16 VALUES (0, [4.6, 2.3]), (1, [2.0, 3.2]), (2, [4.2, 3.4]), (3, [5.3, 2.9]), (4, [2.4, 5.2]), (5, [5.3, 2.3]), (6, [1.0, 9.3]), (7, [5.5, 4.7]), (8, [6.4, 3.5]), (9, [5.3, 2.5]), (10, [6.4, 3.4]), (11, [6.4, 3.2]);
INSERT INTO tab_cos_bf16 VALUES (0, [4.6, 2.3]), (1, [2.0, 3.2]), (2, [4.2, 3.4]), (3, [5.3, 2.9]), (4, [2.4, 5.2]), (5, [5.3, 2.3]), (6, [1.0, 9.3]), (7, [5.5, 4.7]), (8, [6.4, 3.5]), (9, [5.3, 2.5]), (10, [6.4, 3.4]), (11, [6.4, 3.2]);
INSERT INTO tab_cos_i8 VALUES (0, [4.6, 2.3]), (1, [2.0, 3.2]), (2, [4.2, 3.4]), (3, [5.3, 2.9]), (4, [2.4, 5.2]), (5, [5.3, 2.3]), (6, [1.0, 9.3]), (7, [5.5, 4.7]), (8, [6.4, 3.5]), (9, [5.3, 2.5]), (10, [6.4, 3.4]), (11, [6.4, 3.2]);

WITH [0.0, 2.0] AS reference_vec
SELECT id, vec, L2Distance(vec, reference_vec)
FROM tab_l2_f64
ORDER BY L2Distance(vec, reference_vec)
LIMIT 3;

EXPLAIN indexes = 1
WITH [0.0, 2.0] AS reference_vec
SELECT id, vec, L2Distance(vec, reference_vec)
FROM tab_l2_f64
ORDER BY L2Distance(vec, reference_vec)
LIMIT 3;

WITH [0.0, 2.0] AS reference_vec
SELECT id, vec, L2Distance(vec, reference_vec)
FROM tab_l2_f32
ORDER BY L2Distance(vec, reference_vec)
LIMIT 3;

EXPLAIN indexes = 1
WITH [0.0, 2.0] AS reference_vec
SELECT id, vec, L2Distance(vec, reference_vec)
FROM tab_l2_f32
ORDER BY L2Distance(vec, reference_vec)
LIMIT 3;

WITH [0.0, 2.0] AS reference_vec
SELECT id, vec, L2Distance(vec, reference_vec)
FROM tab_l2_f16
ORDER BY L2Distance(vec, reference_vec)
LIMIT 3;

EXPLAIN indexes = 1
WITH [0.0, 2.0] AS reference_vec
SELECT id, vec, L2Distance(vec, reference_vec)
FROM tab_l2_f16
ORDER BY L2Distance(vec, reference_vec)
LIMIT 3;

WITH [0.0, 2.0] AS reference_vec
SELECT id, vec, L2Distance(vec, reference_vec)
FROM tab_l2_bf16
ORDER BY L2Distance(vec, reference_vec)
LIMIT 3;

EXPLAIN indexes = 1
WITH [0.0, 2.0] AS reference_vec
SELECT id, vec, L2Distance(vec, reference_vec)
FROM tab_l2_bf16
ORDER BY L2Distance(vec, reference_vec)
LIMIT 3;

WITH [0.0, 2.0] AS reference_vec
SELECT id, vec, L2Distance(vec, reference_vec)
FROM tab_l2_i8
ORDER BY L2Distance(vec, reference_vec)
LIMIT 3;

EXPLAIN indexes = 1
WITH [0.0, 2.0] AS reference_vec
SELECT id, vec, L2Distance(vec, reference_vec)
FROM tab_l2_i8
ORDER BY L2Distance(vec, reference_vec)
LIMIT 3;

WITH [0.0, 2.0] AS reference_vec
SELECT id, vec, cosineDistance(vec, reference_vec)
FROM tab_cos_f64
ORDER BY cosineDistance(vec, reference_vec)
LIMIT 3;

EXPLAIN indexes = 1
WITH [0.0, 2.0] AS reference_vec
SELECT id, vec, cosineDistance(vec, reference_vec)
FROM tab_cos_f64
ORDER BY cosineDistance(vec, reference_vec)
LIMIT 3;

WITH [0.0, 2.0] AS reference_vec
SELECT id, vec, cosineDistance(vec, reference_vec)
FROM tab_cos_f32
ORDER BY cosineDistance(vec, reference_vec)
LIMIT 3;

EXPLAIN indexes = 1
WITH [0.0, 2.0] AS reference_vec
SELECT id, vec, cosineDistance(vec, reference_vec)
FROM tab_cos_f32
ORDER BY cosineDistance(vec, reference_vec)
LIMIT 3;

WITH [0.0, 2.0] AS reference_vec
SELECT id, vec, cosineDistance(vec, reference_vec)
FROM tab_cos_f16
ORDER BY cosineDistance(vec, reference_vec)
LIMIT 3;

EXPLAIN indexes = 1
WITH [0.0, 2.0] AS reference_vec
SELECT id, vec, cosineDistance(vec, reference_vec)
FROM tab_cos_f16
ORDER BY cosineDistance(vec, reference_vec)
LIMIT 3;

WITH [0.0, 2.0] AS reference_vec
SELECT id, vec, cosineDistance(vec, reference_vec)
FROM tab_cos_bf16
ORDER BY cosineDistance(vec, reference_vec)
LIMIT 3;

EXPLAIN indexes = 1
WITH [0.0, 2.0] AS reference_vec
SELECT id, vec, cosineDistance(vec, reference_vec)
FROM tab_cos_bf16
ORDER BY cosineDistance(vec, reference_vec)
LIMIT 3;

WITH [0.0, 2.0] AS reference_vec
SELECT id, vec, cosineDistance(vec, reference_vec)
FROM tab_cos_i8
ORDER BY cosineDistance(vec, reference_vec)
LIMIT 3;

EXPLAIN indexes = 1
WITH [0.0, 2.0] AS reference_vec
SELECT id, vec, cosineDistance(vec, reference_vec)
FROM tab_cos_i8
ORDER BY cosineDistance(vec, reference_vec)
LIMIT 3;

DROP TABLE tab_l2_f64;
DROP TABLE tab_l2_f32;
DROP TABLE tab_l2_f16;
DROP TABLE tab_l2_bf16;
DROP TABLE tab_l2_i8;
DROP TABLE tab_cos_f64;
DROP TABLE tab_cos_f32;
DROP TABLE tab_cos_f16;
DROP TABLE tab_cos_bf16;
DROP TABLE tab_cos_i8;

SELECT '-- Index on Array(Float64) column';
CREATE TABLE tab(id Int32, vec Array(Float64), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance') GRANULARITY 2) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 3;
INSERT INTO tab VALUES (0, [1.0, 0.0]), (1, [1.1, 0.0]), (2, [1.2, 0.0]), (3, [1.3, 0.0]), (4, [1.4, 0.0]), (5, [1.5, 0.0]), (6, [0.0, 2.0]), (7, [0.0, 2.1]), (8, [0.0, 2.2]), (9, [0.0, 2.3]), (10, [0.0, 2.4]), (11, [0.0, 2.5]);

WITH [0.0, 2.0] AS reference_vec
SELECT id, vec, L2Distance(vec, reference_vec)
FROM tab
ORDER BY L2Distance(vec, reference_vec)
LIMIT 3;

DROP TABLE tab;
