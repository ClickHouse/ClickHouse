-- Tags: no-fasttest, no-ordinary-database, no-parallel-replicas
-- Regression: fused rescoring must preserve a distance expression that is
-- selected explicitly as well as used for ordering.

SET enable_analyzer = 1;
SET parallel_replicas_local_plan = 1;

DROP TABLE IF EXISTS tab_distance_select_fused;
CREATE TABLE tab_distance_select_fused(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO tab_distance_select_fused VALUES
    (0, [1.0, 0.0]),
    (1, [1.1, 0.0]),
    (2, [0.0, 2.0]),
    (3, [0.0, 2.1]),
    (4, [0.0, 2.2]);

SELECT 'distance expression in select list under fused rescoring';
WITH CAST([0.0, 2.0], 'Array(Float32)') AS ref
SELECT id, round(d, 3), toTypeName(d)
FROM
(
    SELECT id, L2Distance(vec, ref) AS d
    FROM tab_distance_select_fused
    ORDER BY L2Distance(vec, ref)
    LIMIT 3
)
SETTINGS vector_search_with_rescoring = 1;

SELECT '-- Expect fused "_distance" rewrite.';
SELECT trimLeft(explain) AS explain FROM
(
    EXPLAIN header = 1
    WITH CAST([0.0, 2.0], 'Array(Float32)') AS ref
    SELECT id, L2Distance(vec, ref) AS d
    FROM tab_distance_select_fused
    ORDER BY L2Distance(vec, ref)
    LIMIT 3
    SETTINGS vector_search_with_rescoring = 1
)
WHERE explain = '_distance Float32'
LIMIT 1;

DROP TABLE tab_distance_select_fused;
