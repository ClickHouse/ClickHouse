-- Tags: no-fasttest, no-ordinary-database, no-parallel-replicas
-- Regression: `dotProduct` uses the vector index with DESC ordering, but fused
-- rescoring stays disabled until its ordering semantics are covered by parity
-- tests against the regular ExpressionStep rerank path.

SET enable_analyzer = 1;
SET parallel_replicas_local_plan = 1;

DROP TABLE IF EXISTS tab_dot_product_fallback;
CREATE TABLE tab_dot_product_fallback(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'dotProduct', 2))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO tab_dot_product_fallback VALUES
    (0, [0.0, 1.0]),
    (1, [1.0, 1.0]),
    (2, [2.0, 2.0]),
    (3, [3.0, 1.0]),
    (4, [1.0, 4.0]);

SELECT 'dotProduct rescoring keeps ExpressionStep path';
WITH CAST([1.0, 2.0], 'Array(Float32)') AS ref
SELECT id, dotProduct(vec, ref) AS d, toTypeName(d)
FROM tab_dot_product_fallback
ORDER BY dotProduct(vec, ref) DESC
LIMIT 3
SETTINGS vector_search_with_rescoring = 1;

SELECT '-- Do not expect fused "_distance" rewrite for dotProduct.';
SELECT trimLeft(explain) AS explain FROM
(
    EXPLAIN header = 1
    WITH CAST([1.0, 2.0], 'Array(Float32)') AS ref
    SELECT id
    FROM tab_dot_product_fallback
    ORDER BY dotProduct(vec, ref) DESC
    LIMIT 3
    SETTINGS vector_search_with_rescoring = 1
)
WHERE explain = '_distance Float32'
LIMIT 1;

DROP TABLE tab_dot_product_fallback;
