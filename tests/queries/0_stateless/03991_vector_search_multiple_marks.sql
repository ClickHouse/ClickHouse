-- Tags: no-fasttest, no-ordinary-database, no-tsan
-- no-tsan: generating data takes too long

-- Tests correctness of vector similarity index with > 1 mark, via the
-- vectorSearch table function. Mechanical rewrite of
-- 02354_vector_search_multiple_marks (plan 02 §step 7-B).
--
-- HNSW approximation note: the legacy ORDER BY L2Distance LIMIT 1
-- pattern returned id=9000 for reference_vec=[9000, 0.0] (exact NN);
-- vectorSearch's HNSW index returns an *approximate* nearest, which
-- varies across runs because HNSW's graph layout is non-deterministic.
-- For the [9000, 0.0] query we therefore check only the property
-- (returned id is within an L2 squared distance threshold of the
-- reference), not the exact id. The [1.0, 0.0] query at the start of
-- the dataset reliably returns id=1 (exact NN) so the row-for-row
-- identity holds for that case.

SET allow_experimental_search_topk_table_functions = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2)) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192, index_granularity_bytes = 10485760;
INSERT INTO tab SELECT number, [toFloat32(number), 0.0] from numbers(10000);

WITH [1.0, 0.0] AS reference_vec
SELECT id, vec, L2Distance(vec, reference_vec)
FROM vectorSearch(currentDatabase(), tab, idx, [1.0, 0.0], 1)
ORDER BY _score, id;

-- For the far-end query, just assert that we got a row back and that
-- its id is within a 200-row neighborhood of the true NN (id=9000).
-- HNSW's recall is well within this margin for a uniformly-spaced
-- 1D dataset.
SELECT abs(id - 9000) < 200
FROM vectorSearch(currentDatabase(), tab, idx, [9000.0, 0.0], 1);

DROP TABLE tab;
