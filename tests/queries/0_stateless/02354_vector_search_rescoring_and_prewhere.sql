-- Tags: no-fasttest, no-ordinary-database, no-parallel-replicas

-- Test for setting 'vector_search_with_rescoring' with filters.

SET enable_analyzer = 1;
SET parallel_replicas_local_plan = 1; -- this setting is randomized, set it explicitly to force local plan for parallel replicas

DROP TABLE IF EXISTS tab;

CREATE TABLE tab (
    id Int32,
    attr1 Int32,
    attr2 Int32,
    vec Array(Float32),
    INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2),
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 2;

INSERT INTO tab VALUES
    (0, 100, 0, [0.311, 0.311]),
    (1, 101, 100, [0.236, 0.236]),
    (2, 102, 200, [0.97, 0.97]),
    (3, 103, 300, [0.369, 0.369]),
    (4, 104, 400, [0.593, 0.593]),
    (5, 105, 500, [0.276, 0.276]),
    (6, 106, 600, [0.58, 0.58]),
    (7, 107, 700, [0.197, 0.197]),
    (8, 108, 800, [0.134, 0.134]),
    (9, 109, 900, [0.484, 0.484]),
    (10, 110, 1000, [0.945, 0.945]),
    (11, 111, 1100, [0.406, 0.406]),
    (12, 112, 1200, [0.105, 0.105]),
    (13, 113, 1300, [0.635, 0.635]),
    (14, 114, 1400, [0.94, 0.94]),
    (15, 115, 1500, [0.655, 0.655]),
    (16, 116, 1600, [0.252, 0.252]),
    (17, 117, 1700, [0.737, 0.737]),
    (18, 118, 1800, [0.612, 0.612]),
    (19, 119, 1900, [0.217, 0.217]);

SELECT 'Reference results without filters';

SELECT id, attr1, attr2, vec
FROM tab
ORDER BY L2Distance(vec, [0.2, 0.3]);

SELECT 'Ensure rescoring optimization works with enabled and disabled PREWHERE';
-- Expect IDs 16 & 19 for next 2 queries

SELECT id
FROM tab
WHERE attr1 > 110
ORDER BY L2Distance(vec, [0.2, 0.3])
LIMIT 4
SETTINGS query_plan_optimize_prewhere = 0,
         optimize_move_to_prewhere = 0;

SELECT id
FROM tab
WHERE attr1 > 110
ORDER BY L2Distance(vec, [0.2, 0.3])
LIMIT 4
SETTINGS query_plan_optimize_prewhere = 1,
         optimize_move_to_prewhere = 1;

SELECT 'Test with enabled rescoring';
-- Expect 16 & 19, and additionally 18 and 17 because they are in the same granules

SELECT id
FROM tab
WHERE attr1 > 110
ORDER BY L2Distance(vec, [0.2, 0.3])
LIMIT 4
SETTINGS vector_search_with_rescoring = 1;

SELECT 'With enabled rescoring and post-filter multiplier = 3, search quality will be slightly different (better)';
SELECT id
FROM tab
WHERE attr1 > 110
ORDER BY L2Distance(vec, [0.2, 0.3])
LIMIT 4
SETTINGS vector_search_with_rescoring = 1,
         vector_search_index_fetch_multiplier = 3;

SELECT 'Check that explicit PREWHERE disables the optimization';
-- Expect no _distance column in result
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN header = 1
    SELECT id
    FROM tab
    PREWHERE attr1 > 110
    ORDER BY L2Distance(vec, [0.2, 0.3])
    LIMIT 4
    )
WHERE (explain LIKE '%_distance%');

SELECT 'Query with explicit PREWHERE works';
SELECT id
FROM tab
PREWHERE attr1 > 110
ORDER BY L2Distance(vec, [0.2, 0.3])
LIMIT 4;

SELECT 'Select all 20 neighbours with the rescoring optimization, distances got from vector index';
SELECT id, attr1, attr2
FROM tab
ORDER BY L2Distance(vec, [0.2, 0.3])
LIMIT 20;

SELECT 'Ensure that optimization was effective for above query, _distance should be seen';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN header = 1
    SELECT id, attr1, attr2, L2Distance(vec, [0.2, 0.3]),
    FROM tab
    ORDER BY L2Distance(vec, [0.2, 0.3])
    LIMIT 20
    )
WHERE (explain LIKE '%_distance%');

SELECT 'Just a test with 2 predicates';
SELECT 'id 16 & 19 will be again output';
SELECT id
FROM tab
WHERE attr1 > 110 AND attr2 > 50
ORDER BY L2Distance(vec, [0.2, 0.3])
LIMIT 4;

DROP TABLE tab;
