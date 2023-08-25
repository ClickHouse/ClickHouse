-- Tags: disabled, no-fasttest, no-ubsan, no-cpu-aarch64, no-upgrade-check

SET allow_experimental_annoy_index = 1;

SELECT '--- Test with Array ---';

DROP TABLE IF EXISTS tab;
CREATE TABLE tab(id Int32, embedding Array(Float32), INDEX annoy_index embedding TYPE annoy()) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity=5;
INSERT INTO tab VALUES (1, [0.0, 0.0, 10.0]), (2, [0.0, 0.0, 10.5]), (3, [0.0, 0.0, 9.5]), (4, [0.0, 0.0, 9.7]), (5, [0.0, 0.0, 10.2]), (6, [10.0, 0.0, 0.0]), (7, [9.5, 0.0, 0.0]), (8, [9.7, 0.0, 0.0]), (9, [10.2, 0.0, 0.0]), (10, [10.5, 0.0, 0.0]), (11, [0.0, 10.0, 0.0]), (12, [0.0, 9.5, 0.0]), (13, [0.0, 9.7, 0.0]), (14, [0.0, 10.2, 0.0]), (15, [0.0, 10.5, 0.0]);

SELECT 'WHERE type, L2Distance';
SELECT *
FROM tab
WHERE L2Distance(embedding, [0.0, 0.0, 10.0]) < 1.0
LIMIT 5;

SELECT 'ORDER BY type, L2Distance';
SELECT *
FROM tab
ORDER BY L2Distance(embedding, [0.0, 0.0, 10.0])
LIMIT 3;

-- Produces different error code with analyzer, TODO: check
-- SELECT 'Reference ARRAYs with non-matching dimension are rejected';
-- SELECT *
-- FROM tab
-- ORDER BY L2Distance(embedding, [0.0, 0.0])
-- LIMIT 3; -- { serverError INCORRECT_QUERY }

SELECT 'WHERE type, L2Distance, check that index is used';
EXPLAIN indexes=1
SELECT *
FROM tab
WHERE L2Distance(embedding, [0.0, 0.0, 10.0]) < 1.0
LIMIT 5;

SELECT 'ORDER BY type, L2Distance, check that index is used';
EXPLAIN indexes=1
SELECT *
FROM tab
ORDER BY L2Distance(embedding, [0.0, 0.0, 10.0])
LIMIT 3;

SELECT 'parameter annoy_index_search_k_nodes';
SELECT *
FROM tab
ORDER BY L2Distance(embedding, [5.3, 7.3, 2.1])
LIMIT 5
SETTINGS annoy_index_search_k_nodes=0; -- searches zero nodes --> no results

SELECT 'parameter max_limit_for_ann_queries';
EXPLAIN indexes=1
SELECT *
FROM tab
ORDER BY L2Distance(embedding, [5.3, 7.3, 2.1])
LIMIT 5
SETTINGS max_limit_for_ann_queries=2; -- doesn't use the ann index

DROP TABLE tab;

SELECT '--- Test with Tuple ---';

CREATE TABLE tab(id Int32, embedding Tuple(Float32, Float32, Float32), INDEX annoy_index embedding TYPE annoy()) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity=5;
INSERT INTO tab VALUES (1, (0.0, 0.0, 10.0)), (2, (0.0, 0.0, 10.5)), (3, (0.0, 0.0, 9.5)), (4, (0.0, 0.0, 9.7)), (5, (0.0, 0.0, 10.2)), (6, (10.0, 0.0, 0.0)), (7, (9.5, 0.0, 0.0)), (8, (9.7, 0.0, 0.0)), (9, (10.2, 0.0, 0.0)), (10, (10.5, 0.0, 0.0)), (11, (0.0, 10.0, 0.0)), (12, (0.0, 9.5, 0.0)), (13, (0.0, 9.7, 0.0)), (14, (0.0, 10.2, 0.0)), (15, (0.0, 10.5, 0.0));

SELECT 'WHERE type, L2Distance';
SELECT *
FROM tab
WHERE L2Distance(embedding, (0.0, 0.0, 10.0)) < 1.0
LIMIT 5;

SELECT 'ORDER BY type, L2Distance';
SELECT *
FROM tab
ORDER BY L2Distance(embedding, (0.0, 0.0, 10.0))
LIMIT 3;

SELECT 'WHERE type, L2Distance, check that index is used';
EXPLAIN indexes=1
SELECT *
FROM tab
WHERE L2Distance(embedding, (0.0, 0.0, 10.0)) < 1.0
LIMIT 5;

SELECT 'ORDER BY type, L2Distance, check that index is used';
EXPLAIN indexes=1
SELECT *
FROM tab
ORDER BY L2Distance(embedding, (0.0, 0.0, 10.0))
LIMIT 3;

SELECT 'parameter annoy_index_search_k_nodes';
SELECT *
FROM tab
ORDER BY L2Distance(embedding, (5.3, 7.3, 2.1))
LIMIT 5
SETTINGS annoy_index_search_k_nodes=0; -- searches zero nodes --> no results

SELECT 'parameter max_limit_for_ann_queries';
EXPLAIN indexes=1
SELECT *
FROM tab
ORDER BY L2Distance(embedding, (5.3, 7.3, 2.1))
LIMIT 5
SETTINGS max_limit_for_ann_queries=2; -- doesn't use the ann index

DROP TABLE tab;

SELECT '--- Test alternative metric (cosine distance) and non-default NumTrees ---';

CREATE TABLE tab(id Int32, embedding Array(Float32), INDEX annoy_index embedding TYPE annoy('cosineDistance', 200)) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity=5;
INSERT INTO tab VALUES (1, [0.0, 0.0, 10.0]), (2, [0.0, 0.0, 10.5]), (3, [0.0, 0.0, 9.5]), (4, [0.0, 0.0, 9.7]), (5, [0.0, 0.0, 10.2]), (6, [10.0, 0.0, 0.0]), (7, [9.5, 0.0, 0.0]), (8, [9.7, 0.0, 0.0]), (9, [10.2, 0.0, 0.0]), (10, [10.5, 0.0, 0.0]), (11, [0.0, 10.0, 0.0]), (12, [0.0, 9.5, 0.0]), (13, [0.0, 9.7, 0.0]), (14, [0.0, 10.2, 0.0]), (15, [0.0, 10.5, 0.0]);

SELECT 'WHERE type, L2Distance';
SELECT *
FROM tab
WHERE L2Distance(embedding, [0.0, 0.0, 10.0]) < 1.0
LIMIT 5;

SELECT 'ORDER BY type, L2Distance';
SELECT *
FROM tab
ORDER BY L2Distance(embedding, [0.0, 0.0, 10.0])
LIMIT 3;

DROP TABLE tab;

SELECT '--- Negative tests ---';

-- must have at most 2 arguments
CREATE TABLE tab(id Int32, embedding Array(Float32), INDEX annoy_index embedding TYPE annoy('too', 'many', 'arguments')) ENGINE = MergeTree ORDER BY id; -- { serverError INCORRECT_QUERY }

-- first argument (distance_function) must be String
CREATE TABLE tab(id Int32, embedding Array(Float32), INDEX annoy_index embedding TYPE annoy(3)) ENGINE = MergeTree ORDER BY id; -- { serverError INCORRECT_QUERY }

-- 2nd argument (number of trees) must be UInt64
CREATE TABLE tab(id Int32, embedding Array(Float32), INDEX annoy_index embedding TYPE annoy('L2Distance', 'not an UInt64')) ENGINE = MergeTree ORDER BY id; -- { serverError INCORRECT_QUERY }

-- reject unsupported distance functions
CREATE TABLE tab(id Int32, embedding Array(Float32), INDEX annoy_index embedding TYPE annoy('wormholeDistance')) ENGINE = MergeTree ORDER BY id; -- { serverError INCORRECT_DATA }

-- must be created on single column
CREATE TABLE tab(id Int32, embedding Array(Float32), INDEX annoy_index (embedding, id) TYPE annoy()) ENGINE = MergeTree ORDER BY id; -- { serverError INCORRECT_NUMBER_OF_COLUMNS }

-- must be created on Array/Tuple(Float32) columns
SET allow_suspicious_low_cardinality_types = 1;
CREATE TABLE tab(id Int32, embedding Float32, INDEX annoy_index embedding TYPE annoy()) ENGINE = MergeTree ORDER BY id; -- { serverError ILLEGAL_COLUMN }
CREATE TABLE tab(id Int32, embedding Array(Float64), INDEX annoy_index embedding TYPE annoy()) ENGINE = MergeTree ORDER BY id; -- { serverError ILLEGAL_COLUMN }
CREATE TABLE tab(id Int32, embedding LowCardinality(Float32), INDEX annoy_index embedding TYPE annoy()) ENGINE = MergeTree ORDER BY id; -- { serverError ILLEGAL_COLUMN }
CREATE TABLE tab(id Int32, embedding Nullable(Float32), INDEX annoy_index embedding TYPE annoy()) ENGINE = MergeTree ORDER BY id; -- { serverError ILLEGAL_COLUMN }
