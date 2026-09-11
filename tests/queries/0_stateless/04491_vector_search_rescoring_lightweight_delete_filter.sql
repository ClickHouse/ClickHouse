-- Tags: no-fasttest, no-ordinary-database, no-parallel-replicas
-- no-parallel-replicas: vector-search read hints are produced during local index analysis.

SET enable_analyzer = 1;
SET mutations_sync = 2;
SET lightweight_deletes_sync = 2;
SET parallel_replicas_local_plan = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt64,
    vec Array(Float32),
    INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2) GRANULARITY 100000000
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8, min_bytes_for_wide_part = 0;

INSERT INTO tab SELECT number, [toFloat32(number), 0] FROM numbers(16);

DELETE FROM tab WHERE id IN (1, 3, 5, 7, 9, 11, 13, 15);

SELECT 'bruteforce reads a surviving granule mate';
WITH [1.0, 0.0] AS reference_vec
SELECT has(groupArray(id), 4)
FROM
(
    SELECT id
    FROM tab
    ORDER BY L2Distance(vec, reference_vec)
    LIMIT 3
    SETTINGS use_skip_indexes = 0
);

-- The row filter restricts the read to the rows the vector index returned, and the index still holds
-- the deleted ones, so applying it while a lightweight delete is unmaterialized would drop candidates
-- with nothing to take their place - the query would return fewer rows than its LIMIT and miss the
-- neighbours a deleted candidate shadowed. The filter is skipped in that case, so the granule mate is
-- rescored and the answer is the same as the bruteforce one.
SELECT 'the same answer with the index';
WITH [1.0, 0.0] AS reference_vec
SELECT groupArray(id)
FROM
(
    SELECT id
    FROM tab
    ORDER BY L2Distance(vec, reference_vec), id
    LIMIT 3
    SETTINGS vector_search_with_rescoring = 1,
             parallel_replicas_local_plan = 1,
             use_skip_indexes_for_top_k = 1
);

DROP TABLE tab;
