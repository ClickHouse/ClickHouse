-- Tags: no-fasttest, no-ordinary-database, no-parallel-replicas
-- Regression: exact row-positioning must preserve a distance expression that
-- is selected explicitly as well as used for ordering.

SET enable_analyzer = 1;
SET parallel_replicas_local_plan = 1;

DROP TABLE IF EXISTS tab_distance_select_rescore;
CREATE TABLE tab_distance_select_rescore(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO tab_distance_select_rescore VALUES
    (0, [1.0, 0.0]),
    (1, [1.1, 0.0]),
    (2, [0.0, 2.0]),
    (3, [0.0, 2.1]),
    (4, [0.0, 2.2]);

SELECT 'distance expression in select list under exact rescoring';
WITH CAST([0.0, 2.0], 'Array(Float32)') AS ref
SELECT id, round(d, 3), toTypeName(d)
FROM
(
    SELECT id, L2Distance(vec, ref) AS d
    FROM tab_distance_select_rescore
    ORDER BY L2Distance(vec, ref)
    LIMIT 3
)
SETTINGS vector_search_with_rescoring = 1;

SELECT '-- Do not expect "_distance" rewrite for exact rescoring.';
SELECT trimLeft(explain) AS explain FROM
(
    EXPLAIN header = 1
    WITH CAST([0.0, 2.0], 'Array(Float32)') AS ref
    SELECT id, L2Distance(vec, ref) AS d
    FROM tab_distance_select_rescore
    ORDER BY L2Distance(vec, ref)
    LIMIT 3
    SETTINGS vector_search_with_rescoring = 1
)
WHERE explain = '_distance Float32';

SELECT 'explicit _distance under exact rescoring remains readable';
WITH CAST([0.0, 2.0], 'Array(Float32)') AS ref
SELECT count(), countIf(_distance < 1)
FROM
(
    SELECT id, _distance
    FROM tab_distance_select_rescore
    ORDER BY L2Distance(vec, ref)
    LIMIT 3
)
SETTINGS vector_search_with_rescoring = 1;

DROP TABLE tab_distance_select_rescore;
