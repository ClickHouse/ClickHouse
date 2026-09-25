-- Tags: no-fasttest, no-ordinary-database, no-parallel-replicas
-- no-parallel-replicas: vector-search read hints are produced during local index analysis.

-- Tests that a PREWHERE filtering on the same distance the query sorts by is rewritten onto the
-- `_distance` virtual column, so the vector column is not read and the distance is not recomputed.

SET enable_analyzer = 1;
SET parallel_replicas_local_plan = 1;

DROP TABLE IF EXISTS tab_cosine;
DROP TABLE IF EXISTS tab_l2;

CREATE TABLE tab_cosine
(
    id UInt64,
    attr UInt64,
    vec Array(Float32),
    INDEX idx_vec vec TYPE vector_similarity('hnsw', 'cosineDistance', 2) GRANULARITY 100000000
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0;

INSERT INTO tab_cosine SELECT number, number % 3, [toFloat32(number), 1] FROM numbers(32);

SELECT 'PREWHERE filters on _distance instead of recomputing the distance';
-- Expect 1: the PREWHERE filter column reads `_distance`, so an explicit PREWHERE no longer
-- disables the optimization.
SELECT count() > 0
FROM
(
    EXPLAIN actions = 1
    WITH [0.0, 1.0] AS reference_vec
    SELECT id
    FROM tab_cosine
    PREWHERE cosineDistance(vec, reference_vec) < 0.5
    ORDER BY cosineDistance(vec, reference_vec)
    LIMIT 3
    SETTINGS vector_search_with_rescoring = 0
)
WHERE explain LIKE '%Prewhere filter column:%_distance%';

SELECT 'and the vector column is dropped from the read list';
-- Expect 1: ReadFromMergeTree reads `_distance` and not `vec`.
SELECT count() > 0
FROM
(
    EXPLAIN actions = 1
    WITH [0.0, 1.0] AS reference_vec
    SELECT id
    FROM tab_cosine
    PREWHERE cosineDistance(vec, reference_vec) < 0.5
    ORDER BY cosineDistance(vec, reference_vec)
    LIMIT 3
    SETTINGS vector_search_with_rescoring = 0
)
WHERE explain LIKE '%Output: _distance%';

SELECT 'and no plan step reads the vector column';
-- Expect 0: `vec` appears in no read list.
SELECT count()
FROM
(
    EXPLAIN actions = 1
    WITH [0.0, 1.0] AS reference_vec
    SELECT id
    FROM tab_cosine
    PREWHERE cosineDistance(vec, reference_vec) < 0.5
    ORDER BY cosineDistance(vec, reference_vec)
    LIMIT 3
    SETTINGS vector_search_with_rescoring = 0
)
WHERE explain LIKE '%Output:%' AND explain LIKE '%vec%';

SELECT 'results match the brute-force baseline';
WITH [0.0, 1.0] AS reference_vec
SELECT id
FROM tab_cosine
PREWHERE cosineDistance(vec, reference_vec) < 0.5
ORDER BY cosineDistance(vec, reference_vec)
LIMIT 3
SETTINGS vector_search_with_rescoring = 0;

WITH [0.0, 1.0] AS reference_vec
SELECT id
FROM tab_cosine
PREWHERE cosineDistance(vec, reference_vec) < 0.5
ORDER BY cosineDistance(vec, reference_vec)
LIMIT 3
SETTINGS use_skip_indexes = 0;

SELECT 'a PREWHERE on a different reference vector must NOT be rewritten';
-- `_distance` only holds the distances belonging to the ORDER BY, so substituting it into a filter
-- over another reference vector would silently return wrong rows. Expect the bail-out.
SELECT count()
FROM
(
    EXPLAIN actions = 1
    SELECT id
    FROM tab_cosine
    PREWHERE cosineDistance(vec, [1.0, 0.0]) < 0.5
    ORDER BY cosineDistance(vec, [0.0, 1.0])
    LIMIT 3
    SETTINGS vector_search_with_rescoring = 0
)
WHERE explain LIKE '%_distance%';

SELECT id
FROM tab_cosine
PREWHERE cosineDistance(vec, [1.0, 0.0]) < 0.5
ORDER BY cosineDistance(vec, [0.0, 1.0])
LIMIT 3
SETTINGS vector_search_with_rescoring = 0;

SELECT id
FROM tab_cosine
PREWHERE cosineDistance(vec, [1.0, 0.0]) < 0.5
ORDER BY cosineDistance(vec, [0.0, 1.0])
LIMIT 3
SETTINGS use_skip_indexes = 0;

SELECT 'a PREWHERE that uses the vector column any other way must NOT be rewritten';
-- The column still has to be read, so there is nothing to gain and the bail-out is kept.
SELECT count()
FROM
(
    EXPLAIN actions = 1
    WITH [0.0, 1.0] AS reference_vec
    SELECT id
    FROM tab_cosine
    PREWHERE length(vec) = 2
    ORDER BY cosineDistance(vec, reference_vec)
    LIMIT 3
    SETTINGS vector_search_with_rescoring = 0
)
WHERE explain LIKE '%_distance%';

SELECT 'a PREWHERE on a non-vector column keeps bailing out';
SELECT count()
FROM
(
    EXPLAIN actions = 1
    WITH [0.0, 1.0] AS reference_vec
    SELECT id
    FROM tab_cosine
    PREWHERE attr = 1
    ORDER BY cosineDistance(vec, reference_vec)
    LIMIT 3
    SETTINGS vector_search_with_rescoring = 0
)
WHERE explain LIKE '%_distance%';

SELECT 'rescoring mode is unaffected';
SELECT count()
FROM
(
    EXPLAIN actions = 1
    WITH [0.0, 1.0] AS reference_vec
    SELECT id
    FROM tab_cosine
    PREWHERE cosineDistance(vec, reference_vec) < 0.5
    ORDER BY cosineDistance(vec, reference_vec)
    LIMIT 3
    SETTINGS vector_search_with_rescoring = 1
)
WHERE explain LIKE '%_distance%';

CREATE TABLE tab_l2
(
    id UInt64,
    vec Array(Float32),
    INDEX idx_vec vec TYPE vector_similarity('hnsw', 'L2Distance', 2) GRANULARITY 100000000
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0;

INSERT INTO tab_l2 SELECT number, [toFloat32(number), 1] FROM numbers(32);

SELECT 'L2Distance is rewritten with the sqrt that usearch skips';
-- usearch returns squared L2 distances, so the rewrite has to wrap `_distance` in sqrt.
SELECT count() > 0
FROM
(
    EXPLAIN actions = 1
    WITH [0.0, 1.0] AS reference_vec
    SELECT id
    FROM tab_l2
    PREWHERE L2Distance(vec, reference_vec) < 3.0
    ORDER BY L2Distance(vec, reference_vec)
    LIMIT 3
    SETTINGS vector_search_with_rescoring = 0
)
WHERE explain LIKE '%Prewhere filter column:%sqrt(_distance)%';

WITH [0.0, 1.0] AS reference_vec
SELECT id
FROM tab_l2
PREWHERE L2Distance(vec, reference_vec) < 3.0
ORDER BY L2Distance(vec, reference_vec)
LIMIT 3
SETTINGS vector_search_with_rescoring = 0;

WITH [0.0, 1.0] AS reference_vec
SELECT id
FROM tab_l2
PREWHERE L2Distance(vec, reference_vec) < 3.0
ORDER BY L2Distance(vec, reference_vec)
LIMIT 3
SETTINGS use_skip_indexes = 0;

SELECT 'a WHERE above the read that needs the vector column keeps bailing out';
-- The rewrite drops `vec` from the read list, so a filter above the read that still consumes it
-- would reference a column that is no longer produced (NOT_FOUND_COLUMN_IN_BLOCK). Note a filter
-- that does not touch the vector column stays eligible, covered by the next case.
SELECT count()
FROM
(
    EXPLAIN actions = 1
    WITH [0.0, 1.0] AS reference_vec
    SELECT id
    FROM tab_cosine
    PREWHERE cosineDistance(vec, reference_vec) < 0.5
    WHERE length(vec) = 2
    ORDER BY cosineDistance(vec, reference_vec)
    LIMIT 3
    SETTINGS vector_search_with_rescoring = 0
)
WHERE explain LIKE '%_distance%';

WITH [0.0, 1.0] AS reference_vec
SELECT id
FROM tab_cosine
PREWHERE cosineDistance(vec, reference_vec) < 0.5
WHERE length(vec) = 2
ORDER BY cosineDistance(vec, reference_vec)
LIMIT 3
SETTINGS vector_search_with_rescoring = 0;

SELECT 'a WHERE that does not touch the vector column is still rewritten';
SELECT count() > 0
FROM
(
    EXPLAIN actions = 1
    WITH [0.0, 1.0] AS reference_vec
    SELECT id
    FROM tab_cosine
    PREWHERE cosineDistance(vec, reference_vec) < 0.5
    WHERE id % 2 = 0
    ORDER BY cosineDistance(vec, reference_vec)
    LIMIT 3
    SETTINGS vector_search_with_rescoring = 0
)
WHERE explain LIKE '%Prewhere filter column:%_distance%';

WITH [0.0, 1.0] AS reference_vec
SELECT id
FROM tab_cosine
PREWHERE cosineDistance(vec, reference_vec) < 0.5
WHERE id % 2 = 0
ORDER BY cosineDistance(vec, reference_vec)
LIMIT 3
SETTINGS vector_search_with_rescoring = 0;

SELECT 'an explicit PREWHERE under FINAL keeps bailing out';
-- FINAL may add PK-overlapping ranges after vector index analysis, and the vector row hints
-- describe only the pre-FINAL candidates. Rewriting there tripped a LOGICAL_ERROR in
-- MergeTreeRangeReader; the rescoring row filter is disabled under FINAL for the same reason.
DROP TABLE IF EXISTS tab_final;

CREATE TABLE tab_final
(
    id UInt64,
    ver UInt64,
    vec Array(Float32),
    INDEX idx_vec vec TYPE vector_similarity('hnsw', 'cosineDistance', 2) GRANULARITY 100000000
)
ENGINE = ReplacingMergeTree(ver)
ORDER BY id
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0;

INSERT INTO tab_final SELECT number, 1, [toFloat32(number), 1] FROM numbers(32);
INSERT INTO tab_final SELECT number, 2, [toFloat32(number + 100), 1] FROM numbers(16);

SELECT count()
FROM
(
    EXPLAIN actions = 1
    WITH [0.0, 1.0] AS reference_vec
    SELECT id
    FROM tab_final FINAL
    PREWHERE cosineDistance(vec, reference_vec) < 0.5
    ORDER BY cosineDistance(vec, reference_vec)
    LIMIT 3
    SETTINGS vector_search_with_rescoring = 0
)
WHERE explain LIKE '%_distance%';

-- Must agree with the rescoring path, which bails out under FINAL already.
WITH [0.0, 1.0] AS reference_vec
SELECT id
FROM tab_final FINAL
PREWHERE cosineDistance(vec, reference_vec) < 0.5
ORDER BY cosineDistance(vec, reference_vec)
LIMIT 3
SETTINGS vector_search_with_rescoring = 0;

WITH [0.0, 1.0] AS reference_vec
SELECT id
FROM tab_final FINAL
PREWHERE cosineDistance(vec, reference_vec) < 0.5
ORDER BY cosineDistance(vec, reference_vec)
LIMIT 3
SETTINGS vector_search_with_rescoring = 1;

DROP TABLE tab_cosine;
DROP TABLE tab_l2;
DROP TABLE tab_final;
