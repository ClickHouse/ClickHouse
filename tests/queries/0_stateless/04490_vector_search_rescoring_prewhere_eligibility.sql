-- Tags: no-fasttest, no-ordinary-database, no-parallel-replicas
-- no-parallel-replicas: vector-search read hints are produced during local index analysis.

SET explain_query_plan_default = 'legacy';
SET enable_analyzer = 1;
SET parallel_replicas_local_plan = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt64,
    attr UInt64,
    payload String,
    vec Array(Float32),
    INDEX idx_vec vec TYPE vector_similarity('hnsw', 'L2Distance', 2) GRANULARITY 100000000
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, max_bytes_to_merge_at_max_space_in_pool = 1;

SYSTEM STOP MERGES tab;

INSERT INTO tab
SELECT number, number % 3, repeat('x', 64), [toFloat32(number), 0]
FROM numbers(32);

SELECT count() > 0
FROM
(
    EXPLAIN actions = 1
    WITH [0.0, 0.0] AS reference_vec
    SELECT id
    FROM tab
    WHERE id = 31 AND attr = 1
    ORDER BY L2Distance(vec, reference_vec)
    LIMIT 1
    SETTINGS vector_search_with_rescoring = 1,
             optimize_move_to_prewhere = 1,
             query_plan_optimize_prewhere = 1
)
WHERE explain LIKE '%Prewhere filter column:%';

SELECT count() = 0
FROM
(
    EXPLAIN actions = 1
    WITH [0.0, 0.0] AS reference_vec
    SELECT id
    FROM tab
    WHERE attr = 1
    ORDER BY L2Distance(vec, reference_vec)
    LIMIT 3
    SETTINGS vector_search_with_rescoring = 1,
             optimize_move_to_prewhere = 1,
             query_plan_optimize_prewhere = 1
)
WHERE explain LIKE '%Prewhere filter column:%';

SELECT count() > 0
FROM
(
    EXPLAIN actions = 1
    WITH [0.0, 0.0] AS reference_vec
    SELECT id, vec
    FROM tab
    WHERE attr = 1
    ORDER BY L2Distance(vec, reference_vec)
    LIMIT 3
    SETTINGS vector_search_with_rescoring = 0,
             optimize_move_to_prewhere = 1,
             query_plan_optimize_prewhere = 1
)
WHERE explain LIKE '%Prewhere filter column:%';

SELECT count() = 0
FROM
(
    EXPLAIN actions = 1
    WITH [0.0, 0.0] AS reference_vec
    SELECT id, vec
    FROM tab
    WHERE attr = 1
    ORDER BY L2Distance(vec, reference_vec)
    LIMIT 3
    SETTINGS vector_search_with_rescoring = 0,
             optimize_move_to_prewhere = 1,
             query_plan_optimize_prewhere = 0
)
WHERE explain LIKE '%Prewhere filter column:%';

DROP TABLE tab;
