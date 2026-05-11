-- Tags: no-fasttest, no-ordinary-database, no-parallel-replicas
-- Regression: fused rescoring for `cosineDistance` must preserve the regular
-- function's `NaN` semantics for zero query vectors.

SET enable_analyzer = 1;
SET parallel_replicas_local_plan = 1;

DROP TABLE IF EXISTS tab_cosine_zero;
CREATE TABLE tab_cosine_zero(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'cosineDistance', 2))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 2;

INSERT INTO tab_cosine_zero VALUES
    (0, [1.0, 0.0]),
    (1, [0.0, 1.0]),
    (2, [2.0, 0.0]),
    (3, [0.0, 2.0]);

SELECT 'cosine zero query keeps function NaN semantics under fused rescoring';
WITH CAST([0.0, 0.0], 'Array(Float32)') AS ref
SELECT countIf(isNaN(d))
FROM
(
    SELECT id, cosineDistance(vec, ref) AS d
    FROM tab_cosine_zero
    ORDER BY d
    LIMIT 4
)
SETTINGS vector_search_with_rescoring = 1;

SELECT '-- Expect fused "_distance" rewrite.';
SELECT trimLeft(explain) AS explain FROM
(
    EXPLAIN header = 1
    SELECT countIf(isNaN(d))
    FROM
    (
        WITH CAST([0.0, 0.0], 'Array(Float32)') AS ref
        SELECT id, cosineDistance(vec, ref) AS d
        FROM tab_cosine_zero
        ORDER BY d
        LIMIT 4
    )
    SETTINGS vector_search_with_rescoring = 1
)
WHERE explain = '_distance Float32'
LIMIT 1;

DROP TABLE tab_cosine_zero;
