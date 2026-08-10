-- Tags: no-fasttest

-- Per plan 02 §step 2: vectorSearch always emits `_score` with the "lower
-- is closer" convention. ORDER BY _score ASC works for every metric
-- (L2Distance / cosineDistance / dotProduct). For dotProduct, USearch's
-- `metric_ip_gt` already converts ip → 1 - ip on the index side.

SET allow_experimental_search_topk_table_functions = 1;

DROP TABLE IF EXISTS tab_l2;
DROP TABLE IF EXISTS tab_cos;
DROP TABLE IF EXISTS tab_dot;

CREATE TABLE tab_l2(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192;
INSERT INTO tab_l2 VALUES (0, [1.0, 0.0]), (1, [1.1, 0.0]), (2, [0.0, 2.0]), (3, [0.0, 2.1]), (4, [0.5, 0.5]);

CREATE TABLE tab_cos(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'cosineDistance', 2))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192;
INSERT INTO tab_cos VALUES (0, [1.0, 0.0]), (1, [1.0, 0.1]), (2, [0.0, 1.0]), (3, [0.1, 1.0]), (4, [0.5, 0.5]);

CREATE TABLE tab_dot(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'dotProduct', 2))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192;
INSERT INTO tab_dot VALUES (0, [4.6, 2.3]), (1, [2.0, 3.2]), (2, [4.2, 3.4]), (3, [5.3, 2.9]), (4, [2.4, 5.2]);

-- All three queries use ORDER BY _score ASC and produce top-3 nearest.
-- For dotProduct, ASC works because USearch's metric_ip_gt converts ip
-- to (1 - ip), so smaller _score == higher inner product.

SELECT '-- L2Distance: top 3 nearest';
SELECT id
FROM vectorSearch(currentDatabase(), tab_l2, idx, [1.0, 0.0], 3)
ORDER BY _score ASC, id;

SELECT '-- cosineDistance: top 3 nearest';
SELECT id
FROM vectorSearch(currentDatabase(), tab_cos, idx, [1.0, 0.0], 3)
ORDER BY _score ASC, id;

SELECT '-- dotProduct: top 3 nearest (largest inner product = lowest _score)';
SELECT id
FROM vectorSearch(currentDatabase(), tab_dot, idx, [0.0, 2.0], 3)
ORDER BY _score ASC, id;

-- _score is non-negative for L2Distance and cosineDistance (it's a
-- proper distance), and in the [0, 2] range for dotProduct
-- (after USearch's 1 - ip transform on normalized inputs).

SELECT '-- _score is non-negative for L2Distance';
SELECT countIf(NOT (_score >= 0)) = 0
FROM vectorSearch(currentDatabase(), tab_l2, idx, [1.0, 0.0], 5);

SELECT '-- _score is non-negative for cosineDistance';
SELECT countIf(NOT (_score >= 0)) = 0
FROM vectorSearch(currentDatabase(), tab_cos, idx, [1.0, 0.0], 5);

DROP TABLE tab_l2;
DROP TABLE tab_cos;
DROP TABLE tab_dot;
