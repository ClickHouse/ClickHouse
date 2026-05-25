-- Tags: no-fasttest, no-ordinary-database, no-parallel-replicas
-- no-parallel-replicas: If parallel replicas are on, the optimization (no rescoring) may not work.

-- Test for setting 'vector_search_with_rescoring'

SET enable_analyzer = 1;
SET parallel_replicas_local_plan = 1; -- this setting is randomized, set it explicitly to force local plan for parallel replicas

DROP TABLE IF EXISTS tab;

CREATE TABLE tab(id Int32, vec Array(Float32), attr1 Int32, INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2)) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 2;
INSERT INTO tab VALUES (0, [1.0, 0.0], 50), (1, [1.1, 0.0], 50), (2, [1.2, 0.0], 50), (3, [1.3, 0.0], 50), (4, [1.4, 0.0], 50), (5, [0.0, 2.0], 50), (6, [0.0, 2.1], 50), (7, [0.0, 2.2], 50), (8, [0.0, 2.3], 50), (9, [0.0, 2.4], 50);

SELECT 'Test "SELECT id" without and with rescoring';

WITH [0.0, 2.0] AS reference_vec
SELECT id
FROM tab
ORDER BY L2Distance(vec, reference_vec)
LIMIT 3
SETTINGS vector_search_with_rescoring = 0;

SELECT '-- Expect column "_distance" in EXPLAIN. Column "vec" is not expected for ReadFromMergeTree.';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN header = 1
    WITH [0.0, 2.0] AS reference_vec
    SELECT id
    FROM tab
    ORDER BY L2Distance(vec, reference_vec)
    LIMIT 3
    SETTINGS vector_search_with_rescoring = 0)
WHERE (explain = '_distance Float32' OR explain LIKE '%vec%Array%') AND explain NOT LIKE '%L2Distance%'
LIMIT 1;

WITH CAST([0.0, 2.0], 'Array(Float32)') AS reference_vec
SELECT id
FROM tab
ORDER BY L2Distance(vec, reference_vec)
LIMIT 3
SETTINGS vector_search_with_rescoring = 1;

SELECT '-- Do not expect column "_distance" in EXPLAIN. Rescoring keeps the regular distance expression.';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN header = 1
    WITH CAST([0.0, 2.0], 'Array(Float32)') AS reference_vec
    SELECT id
    FROM tab
    ORDER BY L2Distance(vec, reference_vec)
    LIMIT 3
    SETTINGS vector_search_with_rescoring = 1)
WHERE explain = '_distance Float32';

SELECT 'Test exact row-positioning filters before ExpressionStep';

WITH CAST([0.0, 2.0], 'Array(Float32)') AS reference_vec
SELECT id, throwIf(id = 4, 'Expected exact row-positioning before ExpressionStep')
FROM tab
ORDER BY L2Distance(vec, reference_vec)
LIMIT 1
SETTINGS vector_search_with_rescoring = 1,
         query_plan_optimize_lazy_materialization = 0,
         query_plan_execute_functions_after_sorting = 0;

SELECT 'Test "SELECT id, vec" without and with rescoring';

-- SELECTing vec explicitly disables the optimization
WITH [0.0, 2.0] AS reference_vec
SELECT id, vec
FROM tab
ORDER BY L2Distance(vec, reference_vec)
LIMIT 3
SETTINGS vector_search_with_rescoring = 0;

SELECT '-- Do not expect column "_distance" in EXPLAIN.';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN header = 1
    WITH [0.0, 2.0] AS reference_vec
    SELECT id, vec
    FROM tab
    ORDER BY L2Distance(vec, reference_vec)
    LIMIT 3
    SETTINGS vector_search_with_rescoring = 0)
WHERE (explain LIKE '%_distance%');

WITH [0.0, 2.0] AS reference_vec
SELECT id, vec
FROM tab
ORDER BY L2Distance(vec, reference_vec)
LIMIT 3
SETTINGS vector_search_with_rescoring = 1;

SELECT '-- Do not expect column "_distance" in EXPLAIN.';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN header = 1
    WITH [0.0, 2.0] AS reference_vec
    SELECT id, vec
    FROM tab
    ORDER BY L2Distance(vec, reference_vec)
    LIMIT 3
    SETTINGS vector_search_with_rescoring = 1)
WHERE (explain LIKE '%_distance%');

SELECT 'Test optimization in the presence of other predicates';

-- Output will be 0,1,2
WITH [1.0, 0.0] AS reference_vec
SELECT id
FROM tab
WHERE id <= 3
ORDER BY L2Distance(vec, reference_vec)
LIMIT 3
SETTINGS vector_search_with_rescoring = 0;

-- Since filter will select partial ranges from part, brute-force search will select 4,5,6
WITH [1.0, 0.0] AS reference_vec
SELECT id, attr1
FROM tab
WHERE id > 3
ORDER BY L2Distance(vec, reference_vec)
LIMIT 3
SETTINGS vector_search_with_rescoring = 0;

SELECT 'Test for filter that selects full part, optimization will take effect';

SELECT '-- Expect column "_distance" in EXPLAIN. Column "vec" is not expected for ReadFromMergeTree.';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN header = 1
    WITH [0.0, 2.0] AS reference_vec
    SELECT id
    FROM tab
    WHERE id >= 0
    ORDER BY L2Distance(vec, reference_vec)
    LIMIT 5
    SETTINGS vector_search_with_rescoring = 0)
WHERE (explain = '_distance Float32' OR explain LIKE '%vec%Array%') AND explain NOT LIKE '%L2Distance%'
LIMIT 1;

-- Output will be 5,6,7,8,9
WITH [0.0, 2.0] AS reference_vec
SELECT id
FROM tab
WHERE id >= 0
ORDER BY L2Distance(vec, reference_vec)
LIMIT 5
SETTINGS vector_search_with_rescoring = 0;

SELECT 'Predicate on non-PK attribute';
-- Output will be 0,1,2,3,4
WITH [1.0, 0.0] AS reference_vec
SELECT id, attr1
FROM tab
WHERE attr1 >= 50
ORDER BY L2Distance(vec, reference_vec)
LIMIT 5
SETTINGS vector_search_with_rescoring = 0;
