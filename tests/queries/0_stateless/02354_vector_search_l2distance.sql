-- Tags: no-fasttest, no-ordinary-database
SET enable_analyzer = 1;
SET parallel_replicas_local_plan = 1;

DROP TABLE IF EXISTS tab_l2_mismatch;

CREATE TABLE tab_l2_mismatch (
    id Int32,
    vec Array(Float32),
    INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 2;

-- Vectors chosen so L2 and L2-squared are clearly different
-- L2([1,1], [0,0]) = sqrt(2) = 1.41421...
-- L2^2([1,1], [0,0]) = 2
INSERT INTO tab_l2_mismatch VALUES (1, [0.0, 0.0]), (2, [1.0, 0.0]), (3, [0.0, 1.0]), (4, [1.0, 1.0]), (5, [0.0, 3.0]);

-- With rescoring ON: L2Distance function runs on actual data, correct sqrt values
SELECT 'rescoring=true (baseline)';
SELECT id, round(L2Distance(vec, [0.0, 0.0]), 4) as dist
FROM tab_l2_mismatch
ORDER BY L2Distance(vec, [0.0, 0.0])
LIMIT 5
SETTINGS vector_search_with_rescoring = 1;

-- With rescoring OFF (default): optimization replaces L2Distance with _distance from index
-- BUG: _distance contains L2-squared from USearch (l2sq_k metric), NOT sqrt'd Euclidean distance
-- FIX: Added call to sqrt() in the node for _distance
SELECT 'rescoring=false (default, optimization active)';
SELECT id, round(L2Distance(vec, [0.0, 0.0]), 4) as dist
FROM tab_l2_mismatch
ORDER BY L2Distance(vec, [0.0, 0.0])
LIMIT 5
SETTINGS vector_search_with_rescoring = 0;

