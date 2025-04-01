-- Tags: no-fasttest, no-ordinary-database, no-asan
-- no-asan: runs too long

-- Basic tests for vector similarity index stored in compact vs. wide format

SET allow_experimental_vector_similarity_index = 1;

SET parallel_replicas_local_plan=1; -- this setting is randomized, set it explicitly to have local plan for parallel replicas

DROP TABLE IF EXISTS tab_compact;
DROP TABLE IF EXISTS tab_wide;

CREATE TABLE tab_compact(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 3)) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 1e9, min_rows_for_wide_part = 1e9, index_granularity = 1000;
CREATE TABLE tab_wide   (id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 3)) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0,   min_rows_for_wide_part = 0, index_granularity = 1000;

SELECT 'Check part formats';

INSERT INTO tab_compact SELECT number, [toFloat32(intHash32(number) / 4294967295), toFloat32(intHash32(number) / 4294967295), toFloat32(intHash32(number)) / 4294967295] from numbers(10000);
INSERT INTO tab_wide    SELECT number, [toFloat32(intHash32(number)) / 4294967295, toFloat32(intHash32(number)) / 4294967295, toFloat32(intHash32(number)) / 4294967295] from numbers(10000);

SELECT table, part_type FROM system.parts WHERE database = currentDatabase() AND table LIKE 'tab_%' ORDER BY table;

-- (*) Because of HNSW's inherent randomness, we are getting different results between the runs.
--     EXPLAIN indexes = 1 is theoretically affected by the same problem ('Granules: x/10') but the output seems fairly stable

SELECT 'Check tab_compact';

WITH [0.3, 0.4, 0.1] AS reference_vec
SELECT id
FROM tab_compact
ORDER BY L2Distance(vec, reference_vec)
LIMIT 3
FORMAT Null; -- (*)

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    WITH [0.3, 0.4, 0.1] AS reference_vec
    SELECT id
    FROM tab_compact
    ORDER BY L2Distance(vec, reference_vec)
    LIMIT 3
)
WHERE explain LIKE '%vector_similarity%' OR explain LIKE '%Granules:%';

SELECT 'Check tab_wide';

WITH [0.3, 0.4, 0.1] AS reference_vec
SELECT id
FROM tab_wide
ORDER BY L2Distance(vec, reference_vec)
LIMIT 3
FORMAT Null; -- (*)

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    WITH [0.3, 0.4, 0.1] AS reference_vec
    SELECT id
    FROM tab_wide
    ORDER BY L2Distance(vec, reference_vec)
    LIMIT 3
)
WHERE explain LIKE '%vector_similarity%' OR explain LIKE '%Granules:%';

DROP TABLE tab_compact;
DROP TABLE tab_wide;
