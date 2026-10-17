-- Tags: no-fasttest, no-ordinary-database, no-asan
-- no-asan: runs too long

-- Basic tests for vector similarity index stored in compact vs. wide format, respectively full vs. packed parts

SET parallel_replicas_local_plan = 1; -- this setting is randomized, set it explicitly to have local plan for parallel replicas

DROP TABLE IF EXISTS tab_compact_full;
DROP TABLE IF EXISTS tab_wide_full;

CREATE TABLE tab_compact_full(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2) GRANULARITY 2) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 1e9, min_rows_for_wide_part = 1e9, min_bytes_for_full_part_storage = 0, index_granularity = 3;
CREATE TABLE tab_wide_full(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2) GRANULARITY 2) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0, index_granularity = 3;

INSERT INTO tab_compact_full VALUES (0, [1.0, 0.0]), (1, [1.1, 0.0]), (2, [1.2, 0.0]), (3, [1.3, 0.0]), (4, [1.4, 0.0]), (5, [1.5, 0.0]), (6, [0.0, 2.0]), (7, [0.0, 2.1]), (8, [0.0, 2.2]), (9, [0.0, 2.3]), (10, [0.0, 2.4]), (11, [0.0, 2.5]);
INSERT INTO tab_wide_full VALUES (0, [1.0, 0.0]), (1, [1.1, 0.0]), (2, [1.2, 0.0]), (3, [1.3, 0.0]), (4, [1.4, 0.0]), (5, [1.5, 0.0]), (6, [0.0, 2.0]), (7, [0.0, 2.1]), (8, [0.0, 2.2]), (9, [0.0, 2.3]), (10, [0.0, 2.4]), (11, [0.0, 2.5]);

SELECT 'Check part formats';

SELECT table, part_type FROM system.parts WHERE database = currentDatabase() AND table LIKE 'tab_%' ORDER BY table;

SELECT 'Check tab_compact_full';

WITH [0.0, 2.0] AS reference_vec
SELECT id, vec, L2Distance(vec, reference_vec)
FROM tab_compact_full
ORDER BY L2Distance(vec, reference_vec)
LIMIT 3;

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    WITH [0.0, 2.0] AS reference_vec
    SELECT id
    FROM tab_compact_full
    ORDER BY L2Distance(vec, reference_vec)
    LIMIT 3
)
WHERE explain LIKE '%vector_similarity%' OR explain LIKE '%Granules:%';

SELECT 'Check tab_wide_full';

WITH [0.0, 2.0] AS reference_vec
SELECT id, vec, L2Distance(vec, reference_vec)
FROM tab_wide_full
ORDER BY L2Distance(vec, reference_vec)
LIMIT 3;

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    WITH [0.0, 2.0] AS reference_vec
    SELECT id
    FROM tab_wide_full
    ORDER BY L2Distance(vec, reference_vec)
    LIMIT 3
)
WHERE explain LIKE '%vector_similarity%' OR explain LIKE '%Granules:%';

DROP TABLE tab_compact_full;
DROP TABLE tab_wide_full;
