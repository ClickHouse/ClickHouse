-- Tags: no-fasttest, no-ordinary-database

-- Vector similarity (HNSW) index should be used for SELECT DISTINCT ... ORDER BY distance LIMIT N (issue #111343)

SET enable_analyzer = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2) GRANULARITY 2) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 3;
INSERT INTO tab VALUES (1, [1.0, 0.0]), (2, [1.1, 0.0]), (3, [1.2, 0.0]), (4, [1.3, 0.0]), (5, [1.4, 0.0]), (6, [1.5, 0.0]), (7, [1.6, 0.0]), (8, [1.7, 0.0]), (9, [1.8, 0.0]), (10, [1.9, 0.0]), (11, [2.0, 0.0]), (12, [2.1, 0.0]);

-- Each check returns 1 if the vector similarity index 'idx' appears in the plan, 0 otherwise.

SELECT 'DISTINCT should use the vector index';
SELECT count() > 0 FROM (
    EXPLAIN indexes = 1 SELECT DISTINCT id FROM tab ORDER BY L2Distance(vec, [1., 0.]) ASC LIMIT 3
)
WHERE explain ILIKE '%Name: idx%';

SELECT 'DISTINCT on multiple columns should use the vector index';
SELECT count() > 0 FROM (
    EXPLAIN indexes = 1 SELECT DISTINCT id, vec FROM tab ORDER BY L2Distance(vec, [1., 0.]) ASC LIMIT 3
)
WHERE explain ILIKE '%Name: idx%';

SELECT 'DISTINCT with a WHERE clause should use the vector index';
SELECT count() > 0 FROM (
    EXPLAIN indexes = 1 SELECT DISTINCT id FROM tab WHERE id > 2 ORDER BY L2Distance(vec, [1., 0.]) ASC LIMIT 3
)
WHERE explain ILIKE '%Name: idx%';

SELECT 'Non-DISTINCT still uses the vector index';
SELECT count() > 0 FROM (
    EXPLAIN indexes = 1 SELECT id FROM tab ORDER BY L2Distance(vec, [1., 0.]) ASC LIMIT 3
)
WHERE explain ILIKE '%Name: idx%';

-- The old analyzer arranges the DISTINCT plan steps differently, so cover it explicitly.
SET enable_analyzer = 0;
SELECT 'DISTINCT should use the vector index (old analyzer)';
SELECT count() > 0 FROM (
    EXPLAIN indexes = 1 SELECT DISTINCT id FROM tab ORDER BY L2Distance(vec, [1., 0.]) ASC LIMIT 3
)
WHERE explain ILIKE '%Name: idx%';
SET enable_analyzer = 1;

DROP TABLE tab;

-- The index is built over a non-value-preserving expression. DISTINCT (like the non-DISTINCT case)
-- must fall back to brute force, otherwise raw-vector queries would be answered against transformed
-- index data and return wrong nearest neighbours.
DROP TABLE IF EXISTS tab_transformed;
CREATE TABLE tab_transformed(id Int32, vec Array(Float32), INDEX idx arrayMap(x -> 100 - x, vec) TYPE vector_similarity('hnsw', 'L2Distance', 2) GRANULARITY 2) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 3;
INSERT INTO tab_transformed VALUES (1, [1.0, 0.0]), (2, [1.1, 0.0]), (3, [1.2, 0.0]), (4, [1.3, 0.0]), (5, [1.4, 0.0]), (6, [1.5, 0.0]), (7, [1.6, 0.0]), (8, [1.7, 0.0]), (9, [1.8, 0.0]), (10, [1.9, 0.0]), (11, [2.0, 0.0]), (12, [2.1, 0.0]);

SELECT 'DISTINCT over a non-value-preserving index expression should be brute force';
SELECT count() > 0 FROM (
    EXPLAIN indexes = 1 SELECT DISTINCT id FROM tab_transformed ORDER BY L2Distance(vec, [1., 0.]) ASC LIMIT 3
)
WHERE explain ILIKE '%Name: idx%';

DROP TABLE tab_transformed;

-- Correctness: with enough over-fetch (vector_search_index_fetch_multiplier), the indexed DISTINCT result
-- matches the brute-force result even when duplicate projection values collapse below the LIMIT.
DROP TABLE IF EXISTS tab_idx;
DROP TABLE IF EXISTS tab_bruteforce;
CREATE TABLE tab_idx(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2) GRANULARITY 2) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 3;
CREATE TABLE tab_bruteforce(id Int32, vec Array(Float32)) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 3;
-- id = 1 occupies the three positions nearest to [1., 0.]
INSERT INTO tab_idx VALUES (1, [1.00, 0.0]), (1, [1.01, 0.0]), (1, [1.02, 0.0]), (2, [1.50, 0.0]), (3, [1.60, 0.0]), (4, [1.70, 0.0]), (5, [1.80, 0.0]), (6, [1.90, 0.0]), (7, [2.00, 0.0]), (8, [2.10, 0.0]), (9, [2.20, 0.0]), (10, [2.30, 0.0]);
INSERT INTO tab_bruteforce SELECT * FROM tab_idx;

SELECT 'Brute-force DISTINCT result';
SELECT DISTINCT id FROM tab_bruteforce ORDER BY L2Distance(vec, [1., 0.]) ASC LIMIT 3;
SELECT 'Indexed DISTINCT result (equal with sufficient over-fetch)';
SELECT DISTINCT id FROM tab_idx ORDER BY L2Distance(vec, [1., 0.]) ASC LIMIT 3 SETTINGS vector_search_index_fetch_multiplier = 100;

DROP TABLE tab_idx;
DROP TABLE tab_bruteforce;
