-- Tags: no-fasttest, no-ordinary-database

-- Test for issue #110410: a vector_similarity index built on a value-identity expression of the vector
-- column (e.g. identity(vec), materialize(vec)) must be usable, not silently fall back to brute-force.
-- A non-value-preserving expression (e.g. arrayMap(x -> 100 - x, vec)) must NOT be treated as equivalent
-- to the base column: the index is built over the transformed vectors while the query searches with the
-- raw reference vector, so using it would return the wrong top-K. It falls back to a correct brute-force.

SET explain_query_plan_default = 'legacy';

SET parallel_replicas_local_plan = 1; -- this setting is randomized, set it explicitly to have local plan for parallel replicas

DROP TABLE IF EXISTS tab_id;
DROP TABLE IF EXISTS tab_bad;

CREATE TABLE tab_id(id Int32, vec Array(Float32), INDEX idx identity(vec) TYPE vector_similarity('hnsw', 'L2Distance', 2)) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 2;
INSERT INTO tab_id VALUES (0, [0.0, 0.0]), (1, [1.0, 1.0]), (2, [2.0, 2.0]), (3, [3.0, 3.0]), (4, [10.0, 10.0]), (5, [20.0, 20.0]);

SELECT 'Index on a value-identity expression of vec is used';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1 SELECT id FROM tab_id ORDER BY L2Distance(vec, [0.0, 0.0]) LIMIT 3
)
WHERE explain ILIKE '%Skip%' OR explain ILIKE '%Name: idx%' OR explain ILIKE '%vector_similarity%';

SELECT 'Value-identity expression index returns the correct top-K';
SELECT id FROM tab_id ORDER BY L2Distance(vec, [0.0, 0.0]) LIMIT 3 SETTINGS vector_search_with_rescoring = 0;

-- Index built over 100 - vec, but the query searches with the raw reference vector [0, 0].
CREATE TABLE tab_bad(id Int32, vec Array(Float32), INDEX idx arrayMap(x -> 100 - x, vec) TYPE vector_similarity('hnsw', 'L2Distance', 2)) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 2;
INSERT INTO tab_bad VALUES (0, [0.0, 0.0]), (1, [1.0, 1.0]), (2, [2.0, 2.0]), (3, [3.0, 3.0]), (4, [10.0, 10.0]), (5, [20.0, 20.0]);

SELECT 'Non-value-preserving expression index is NOT used (would give wrong top-K)';
SELECT countIf(explain ILIKE '%Skip%') FROM (
    EXPLAIN indexes=1 SELECT id FROM tab_bad ORDER BY L2Distance(vec, [0.0, 0.0]) LIMIT 3
);

SELECT 'Non-value-preserving expression index still returns the correct top-K (brute-force)';
SELECT id FROM tab_bad ORDER BY L2Distance(vec, [0.0, 0.0]) LIMIT 3 SETTINGS vector_search_with_rescoring = 0;

-- A table may carry both a value-preserving and a non-value-preserving vector index on the same column.
-- Only the value-preserving one may be used; the non-value-preserving one must be rejected so it cannot
-- return the wrong top-K.
DROP TABLE IF EXISTS tab_mixed;
CREATE TABLE tab_mixed(id Int32, vec Array(Float32),
    INDEX idx_bad arrayMap(x -> 100 - x, vec) TYPE vector_similarity('hnsw', 'L2Distance', 2),
    INDEX idx_good vec TYPE vector_similarity('hnsw', 'L2Distance', 2)) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192;
INSERT INTO tab_mixed SELECT number, [toFloat32(number), toFloat32(number)] FROM numbers(200);

SELECT 'With both index kinds on the same column, only the value-preserving index is used';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1 SELECT id FROM tab_mixed ORDER BY L2Distance(vec, [0.0, 0.0]) LIMIT 5
)
WHERE explain ILIKE '%Name: idx%';

SELECT 'Mixed-index table returns the correct top-K';
SELECT id FROM tab_mixed ORDER BY L2Distance(vec, [0.0, 0.0]) LIMIT 5 SETTINGS vector_search_with_rescoring = 0;

DROP TABLE tab_id;
DROP TABLE tab_bad;
DROP TABLE tab_mixed;
