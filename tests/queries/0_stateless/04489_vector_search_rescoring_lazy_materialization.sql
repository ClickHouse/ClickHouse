-- Tags: no-fasttest, no-ordinary-database, no-parallel-replicas
-- no-parallel-replicas: vector-search read hints are produced during local index analysis.

SET explain_query_plan_default = 'legacy';
SET enable_analyzer = 1;
SET parallel_replicas_local_plan = 1;
SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_max_limit_for_lazy_materialization = 1000;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt64,
    payload String,
    vec Array(Float32),
    INDEX idx_vec vec TYPE vector_similarity('hnsw', 'L2Distance', 2) GRANULARITY 100000000
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, max_bytes_to_merge_at_max_space_in_pool = 1;

SYSTEM STOP MERGES tab;

INSERT INTO tab
SELECT number, repeat('x', 32), [toFloat32(number), 0]
FROM numbers(12);

SELECT trimLeft(explain)
FROM
(
    EXPLAIN
    WITH [0.0, 0.0] AS reference_vec
    SELECT id, payload
    FROM tab
    ORDER BY L2Distance(vec, reference_vec)
    LIMIT 3
    SETTINGS vector_search_with_rescoring = 1
)
WHERE explain LIKE '%LazilyReadFromMergeTree%';

SELECT trimLeft(explain)
FROM
(
    EXPLAIN indexes = 1
    WITH [0.0, 0.0] AS reference_vec
    SELECT id, payload
    FROM tab
    ORDER BY L2Distance(vec, reference_vec)
    LIMIT 3
    SETTINGS vector_search_with_rescoring = 1
)
WHERE explain LIKE '%Name: idx_vec%';

WITH [0.0, 0.0] AS reference_vec
SELECT id, length(payload)
FROM tab
ORDER BY L2Distance(vec, reference_vec)
LIMIT 3
SETTINGS vector_search_with_rescoring = 1;

-- Regression: a reference vector with a huge component saturates every L2Distance to the
-- same value, so the vector index returns a tie whose candidate set can differ from the rows
-- the LIMIT keeps. The lazy read must re-fetch exactly the selected rows and must not
-- re-apply the vector-index candidate filter; otherwise the lazy chunk ends up shorter than
-- the offsets and prepareLazyChunk hits a LOGICAL_ERROR. All payloads are equal, so the
-- selected rows are irrelevant: the query must simply return LIMIT rows.
WITH [1000.0001, 3.4028234663852886e38] AS reference_vec
SELECT length(payload)
FROM tab
ORDER BY L2Distance(vec, reference_vec)
LIMIT 3
SETTINGS vector_search_with_rescoring = 1;

DROP TABLE tab;
