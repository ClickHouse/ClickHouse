-- Tags: no-fasttest, no-ordinary-database, no-parallel-replicas
-- Regression: fused rescoring must not replace a `Float64` distance expression
-- with the `Float32` `_distance` virtual column.

SET enable_analyzer = 1;
SET parallel_replicas_local_plan = 1;

DROP TABLE IF EXISTS tab_l2_f64_ref;
CREATE TABLE tab_l2_f64_ref(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 2;

INSERT INTO tab_l2_f64_ref VALUES
    (0, [1.0, 0.0]),
    (1, [2.0, 0.0]),
    (2, [3.0, 0.0]),
    (3, [4.0, 0.0]);

SELECT 'Float64 reference keeps ExpressionStep rescoring precision';
WITH [1.00000001, 0.0] AS ref
SELECT id, d > 0 AND d < 0.0000001, toTypeName(d)
FROM
(
    SELECT id, L2Distance(vec, ref) AS d
    FROM tab_l2_f64_ref
    ORDER BY d
    LIMIT 1
)
SETTINGS vector_search_with_rescoring = 1;

SELECT '-- Do not expect fused "_distance" rewrite for Float64 distance result.';
SELECT trimLeft(explain) AS explain FROM
(
    EXPLAIN header = 1
    WITH [1.00000001, 0.0] AS ref
    SELECT id, L2Distance(vec, ref) AS d
    FROM tab_l2_f64_ref
    ORDER BY d
    LIMIT 1
    SETTINGS vector_search_with_rescoring = 1
)
WHERE explain = '_distance Float32'
LIMIT 1;

DROP TABLE tab_l2_f64_ref;
