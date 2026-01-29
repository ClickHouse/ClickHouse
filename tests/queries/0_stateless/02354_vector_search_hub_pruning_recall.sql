-- Tags: no-fasttest, no-ordinary-database
-- Test to validate that hub node pruning (LeaNN optimization) works correctly
-- and that indexes with pruning can still perform vector searches.
-- Note: Full recall validation requires larger datasets and is better suited
-- for the Python stress test framework (ci/jobs/vector_search_stress_tests.py).

SET enable_analyzer = 1;
SET allow_experimental_leann_optimization_for_hnsw = 1;

-- Test that hub pruning creates a smaller index and still allows searches
DROP TABLE IF EXISTS tab_no_pruning;
CREATE TABLE tab_no_pruning(id Int32, vec Array(Float32), 
    INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2, 'f32', 16, 32, 'ratio=0.0')
) ENGINE = MergeTree ORDER BY id;

DROP TABLE IF EXISTS tab_with_pruning;
CREATE TABLE tab_with_pruning(id Int32, vec Array(Float32),
    INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2, 'f32', 16, 32, 'ratio=0.5')
) ENGINE = MergeTree ORDER BY id;

-- Insert test vectors
INSERT INTO tab_no_pruning VALUES
    (0, [0.0, 0.0]), (1, [0.1, 0.1]), (2, [0.2, 0.2]), (3, [0.3, 0.3]), (4, [0.4, 0.4]),
    (5, [1.0, 0.0]), (6, [1.1, 0.1]), (7, [1.2, 0.2]), (8, [1.3, 0.3]), (9, [1.4, 0.4]),
    (10, [0.0, 1.0]), (11, [0.1, 1.1]), (12, [0.2, 1.2]), (13, [0.3, 1.3]), (14, [0.4, 1.4]),
    (15, [1.0, 1.0]), (16, [1.1, 1.1]), (17, [1.2, 1.2]), (18, [1.3, 1.3]), (19, [1.4, 1.4]);

INSERT INTO tab_with_pruning SELECT id, vec FROM tab_no_pruning;

OPTIMIZE TABLE tab_no_pruning FINAL;
OPTIMIZE TABLE tab_with_pruning FINAL;

-- Verify both indexes can perform searches
SELECT '-- Testing search with index without pruning';
WITH [0.0, 0.0] AS query_vec
SELECT id, L2Distance(vec, query_vec) AS dist
FROM tab_no_pruning
ORDER BY dist
LIMIT 5;

SELECT '-- Testing search with index with hub pruning';
WITH [0.0, 0.0] AS query_vec
SELECT id, L2Distance(vec, query_vec) AS dist
FROM tab_with_pruning
ORDER BY dist
LIMIT 5;

-- Compare index sizes (pruned index should be smaller)
SELECT '-- Index size comparison';
SELECT 
    'No pruning: ' || toString(data_uncompressed_bytes) || ' bytes' AS size_no_pruning
FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 'tab_no_pruning' AND name = 'idx';

SELECT 
    'With pruning: ' || toString(data_uncompressed_bytes) || ' bytes' AS size_with_pruning
FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 'tab_with_pruning' AND name = 'idx';

DROP TABLE IF EXISTS tab_no_pruning;
DROP TABLE IF EXISTS tab_with_pruning;
