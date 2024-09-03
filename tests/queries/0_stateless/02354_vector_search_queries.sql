-- Tags: no-fasttest, no-ubsan, no-cpu-aarch64, no-ordinary-database, no-asan

-- Tests vector search in ClickHouse, i.e. Annoy and Usearch indexes. Both index types share similarities in implementation and usage,
-- therefore they are tested in a single file.

-- This file tests various simple approximate nearest neighborhood (ANN) queries that utilize vector search indexes.

SET allow_experimental_annoy_index = 1;
SET allow_experimental_usearch_index = 1;

SELECT 'ARRAY, 10 rows, index_granularity = 8192, GRANULARITY = 1 million --> 1 granule, 1 indexed block';

DROP TABLE IF EXISTS tab_annoy;
DROP TABLE IF EXISTS tab_usearch;

CREATE TABLE tab_annoy(id Int32, vec Array(Float32), INDEX idx vec TYPE annoy()) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192;
INSERT INTO tab_annoy VALUES (0, [1.0, 0.0]), (1, [1.1, 0.0]), (2, [1.2, 0.0]), (3, [1.3, 0.0]), (4, [1.4, 0.0]), (5, [0.0, 2.0]), (6, [0.0, 2.1]), (7, [0.0, 2.2]), (8, [0.0, 2.3]), (9, [0.0, 2.4]);

CREATE TABLE tab_usearch(id Int32, vec Array(Float32), INDEX idx vec TYPE usearch()) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192;
INSERT INTO tab_usearch VALUES (0, [1.0, 0.0]), (1, [1.1, 0.0]), (2, [1.2, 0.0]), (3, [1.3, 0.0]), (4, [1.4, 0.0]), (5, [0.0, 2.0]), (6, [0.0, 2.1]), (7, [0.0, 2.2]), (8, [0.0, 2.3]), (9, [0.0, 2.4]);


SELECT '- Annoy: WHERE-type';
WITH [0.0, 2.0] AS reference_vec
SELECT id, vec, L2Distance(vec, reference_vec)
FROM tab_annoy
WHERE L2Distance(vec, reference_vec) < 1.0
LIMIT 3;

SELECT '- Annoy: ORDER-BY-type';
WITH [0.0, 2.0] AS reference_vec
SELECT id, vec, L2Distance(vec, reference_vec)
FROM tab_annoy
ORDER BY L2Distance(vec, reference_vec)
LIMIT 3;

SELECT '- Usearch: WHERE-type';
WITH [0.0, 2.0] AS reference_vec
SELECT id, vec, L2Distance(vec, reference_vec)
FROM tab_usearch
WHERE L2Distance(vec, reference_vec) < 1.0
LIMIT 3;

SELECT '- Usearch: ORDER-BY-type';
WITH [0.0, 2.0] AS reference_vec
SELECT id, vec, L2Distance(vec, reference_vec)
FROM tab_usearch
ORDER BY L2Distance(vec, reference_vec)
LIMIT 3;

SELECT '- Annoy: WHERE-type, EXPLAIN';
EXPLAIN indexes=1
WITH [0.0, 2.0] AS reference_vec
SELECT id, vec, L2Distance(vec, reference_vec)
FROM tab_annoy
WHERE L2Distance(vec, reference_vec) < 1.0
LIMIT 3;

SELECT '- Annoy: ORDER-BY-type, EXPLAIN';
EXPLAIN indexes=1
WITH [0.0, 2.0] AS reference_vec
SELECT id, vec, L2Distance(vec, reference_vec)
FROM tab_annoy
ORDER BY L2Distance(vec, reference_vec)
LIMIT 3;

SELECT '- Usearch: WHERE-type, EXPLAIN';
EXPLAIN indexes=1
WITH [0.0, 2.0] AS reference_vec
SELECT id, vec, L2Distance(vec, reference_vec)
FROM tab_usearch
WHERE L2Distance(vec, reference_vec) < 1.0
LIMIT 3;

SELECT '- Usearch: ORDER-BY-type, EXPLAIN';
EXPLAIN indexes=1
WITH [0.0, 2.0] AS reference_vec
SELECT id, vec, L2Distance(vec, reference_vec)
FROM tab_usearch
ORDER BY L2Distance(vec, reference_vec)
LIMIT 3;

DROP TABLE tab_annoy;
DROP TABLE tab_usearch;


SELECT 'ARRAY vectors, 12 rows, index_granularity = 3, GRANULARITY = 2 --> 4 granules, 2 indexed block';

CREATE TABLE tab_annoy(id Int32, vec Array(Float32), INDEX idx vec TYPE annoy() GRANULARITY 2) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 3;
INSERT INTO tab_annoy VALUES (0, [1.0, 0.0]), (1, [1.1, 0.0]), (2, [1.2, 0.0]), (3, [1.3, 0.0]), (4, [1.4, 0.0]), (5, [1.5, 0.0]), (6, [0.0, 2.0]), (7, [0.0, 2.1]), (8, [0.0, 2.2]), (9, [0.0, 2.3]), (10, [0.0, 2.4]), (11, [0.0, 2.5]);

CREATE TABLE tab_usearch(id Int32, vec Array(Float32), INDEX idx vec TYPE usearch() GRANULARITY 2) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 3;
INSERT INTO tab_usearch VALUES (0, [1.0, 0.0]), (1, [1.1, 0.0]), (2, [1.2, 0.0]), (3, [1.3, 0.0]), (4, [1.4, 0.0]), (5, [1.5, 0.0]), (6, [0.0, 2.0]), (7, [0.0, 2.1]), (8, [0.0, 2.2]), (9, [0.0, 2.3]), (10, [0.0, 2.4]), (11, [0.0, 2.5]);

SELECT '- Annoy: WHERE-type';
WITH [0.0, 2.0] AS reference_vec
SELECT id, vec, L2Distance(vec, reference_vec)
FROM tab_annoy
WHERE L2Distance(vec, reference_vec) < 1.0
LIMIT 3;

SELECT '- Annoy: ORDER-BY-type';
WITH [0.0, 2.0] AS reference_vec
SELECT id, vec, L2Distance(vec, reference_vec)
FROM tab_annoy
ORDER BY L2Distance(vec, reference_vec)
LIMIT 3;

SELECT '- Usearch: WHERE-type';
WITH [0.0, 2.0] AS reference_vec
SELECT id, vec, L2Distance(vec, reference_vec)
FROM tab_usearch
WHERE L2Distance(vec, reference_vec) < 1.0
LIMIT 3;

SELECT '- Usearch: ORDER-BY-type';
WITH [0.0, 2.0] AS reference_vec
SELECT id, vec, L2Distance(vec, reference_vec)
FROM tab_usearch
ORDER BY L2Distance(vec, reference_vec)
LIMIT 3;

SELECT '- Annoy: WHERE-type, EXPLAIN';
EXPLAIN indexes=1
WITH [0.0, 2.0] AS reference_vec
SELECT id, vec, L2Distance(vec, reference_vec)
FROM tab_annoy
WHERE L2Distance(vec, reference_vec) < 1.0
LIMIT 3;

SELECT '- Annoy: ORDER-BY-type, EXPLAIN';
EXPLAIN indexes=1
WITH [0.0, 2.0] AS reference_vec
SELECT id, vec, L2Distance(vec, reference_vec)
FROM tab_annoy
ORDER BY L2Distance(vec, reference_vec)
LIMIT 3;

SELECT '- Usearch: WHERE-type, EXPLAIN';
EXPLAIN indexes=1
WITH [0.0, 2.0] AS reference_vec
SELECT id, vec, L2Distance(vec, reference_vec)
FROM tab_usearch
WHERE L2Distance(vec, reference_vec) < 1.0
LIMIT 3;

SELECT '- Usearch: ORDER-BY-type, EXPLAIN';
EXPLAIN indexes=1
WITH [0.0, 2.0] AS reference_vec
SELECT id, vec, L2Distance(vec, reference_vec)
FROM tab_usearch
ORDER BY L2Distance(vec, reference_vec)
LIMIT 3;

DROP TABLE tab_annoy;
DROP TABLE tab_usearch;


SELECT 'TUPLE vectors and special cases';
-- Not a systematic test, just to check that no bad things happen.
-- Just for jun, use metric = 'cosineDistance' (Annoy/Usearch), tree_count = 200 (Annoy), scalarKind = 'f64' (Usearch)

CREATE TABLE tab_annoy(id Int32, vec Tuple(Float32, Float32), INDEX idx vec TYPE annoy('cosineDistance', 200) GRANULARITY 2) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 3;
INSERT INTO tab_annoy VALUES (0, (4.6, 2.3)), (1, (2.0, 3.2)), (2, (4.2, 3.4)), (3, (5.3, 2.9)), (4, (2.4, 5.2)), (5, (5.3, 2.3)), (6, (1.0, 9.3)), (7, (5.5, 4.7)), (8, (6.4, 3.5)), (9, (5.3, 2.5)), (10, (6.4, 3.4)), (11, (6.4, 3.2));

CREATE TABLE tab_usearch(id Int32, vec Tuple(Float32, Float32), INDEX idx vec TYPE usearch('cosineDistance', 'f64') GRANULARITY 2) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 3;
INSERT INTO tab_usearch VALUES (0, (4.6, 2.3)), (1, (2.0, 3.2)), (2, (4.2, 3.4)), (3, (5.3, 2.9)), (4, (2.4, 5.2)), (5, (5.3, 2.3)), (6, (1.0, 9.3)), (7, (5.5, 4.7)), (8, (6.4, 3.5)), (9, (5.3, 2.5)), (10, (6.4, 3.4)), (11, (6.4, 3.2));

SELECT '- Annoy: WHERE-type';
WITH (0.0, 2.0) AS reference_vec
SELECT id, vec, cosineDistance(vec, reference_vec)
FROM tab_annoy
WHERE cosineDistance(vec, reference_vec) < 1.0
LIMIT 3;

SELECT '- Annoy: ORDER-BY-type';
WITH (0.0, 2.0) AS reference_vec
SELECT id, vec, cosineDistance(vec, reference_vec)
FROM tab_annoy
ORDER BY cosineDistance(vec, reference_vec)
LIMIT 3;

SELECT '- Usearch: WHERE-type';
WITH (0.0, 2.0) AS reference_vec
SELECT id, vec, cosineDistance(vec, reference_vec)
FROM tab_usearch
WHERE cosineDistance(vec, reference_vec) < 1.0
LIMIT 3;

SELECT '- Usearch: ORDER-BY-type';
WITH (0.0, 2.0) AS reference_vec
SELECT id, vec, cosineDistance(vec, reference_vec)
FROM tab_usearch
ORDER BY cosineDistance(vec, reference_vec)
LIMIT 3;

SELECT '- Special case: MaximumDistance is negative';
WITH (0.0, 2.0) as reference_vec
SELECT id, vec, cosineDistance(vec, reference_vec)
FROM tab_annoy
WHERE cosineDistance(vec, reference_vec) < -1.0
LIMIT 3; -- { serverError INCORRECT_QUERY }

SELECT '- Special case: MaximumDistance is negative';
WITH (0.0, 2.0) as reference_vec
SELECT id, vec, cosineDistance(vec, reference_vec)
FROM tab_usearch
WHERE cosineDistance(vec, reference_vec) < -1.0
LIMIT 3; -- { serverError INCORRECT_QUERY }

SELECT '- Special case: setting "annoy_index_search_k_nodes"';
WITH (0.0, 2.0) as reference_vec
SELECT id, vec, cosineDistance(vec, reference_vec)
FROM tab_annoy
ORDER BY cosineDistance(vec, reference_vec)
LIMIT 3
SETTINGS annoy_index_search_k_nodes=0; -- searches zero nodes --> no results

SELECT '- Special case: setting "max_limit_for_ann_queries"';
EXPLAIN indexes=1
WITH (0.0, 2.0) as reference_vec
SELECT id, vec, cosineDistance(vec, reference_vec)
FROM tab_annoy
ORDER BY cosineDistance(vec, reference_vec)
LIMIT 3
SETTINGS max_limit_for_ann_queries=2; -- LIMIT 3 > 2 --> don't use the ann index

SELECT '- Special case: setting "max_limit_for_ann_queries"';
EXPLAIN indexes=1
WITH (0.0, 2.0) as reference_vec
SELECT id, vec, cosineDistance(vec, reference_vec)
FROM tab_usearch
ORDER BY cosineDistance(vec, reference_vec)
LIMIT 3
SETTINGS max_limit_for_ann_queries=2; -- LIMIT 3 > 2 --> don't use the ann index

DROP TABLE tab_annoy;
DROP TABLE tab_usearch;
