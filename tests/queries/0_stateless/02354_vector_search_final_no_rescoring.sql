-- Tags: no-fasttest
-- no-fasttest: the vector similarity index (USearch) is not built in the fast test.

-- Vector search without rescoring under FINAL must return the visible version of each row: a newer version (or an
-- `is_deleted` row) that is not a vector index candidate of its own part must still replace the older version.

SET enable_parallel_replicas = 0;
-- The index has to be used under FINAL, and exact mode keeps the newer versions of the selected keys.
SET use_skip_indexes = 1, use_skip_indexes_if_final = 1, use_skip_indexes_if_final_exact_mode = 1;
SET vector_search_with_rescoring = 0;

DROP TABLE IF EXISTS tab;
DROP TABLE IF EXISTS tab_sparse;
DROP TABLE IF EXISTS tab_deleted;

CREATE TABLE tab (id UInt64, ver UInt64, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2) GRANULARITY 100000000)
ENGINE = ReplacingMergeTree(ver) ORDER BY id SETTINGS index_granularity = 4;
SYSTEM STOP MERGES tab;
INSERT INTO tab VALUES (1, 1, [0, 0]), (10, 1, [50, 0]), (11, 1, [60, 0]), (12, 1, [70, 0]);
-- Moves id 1 away from the reference vector.
INSERT INTO tab VALUES (1, 2, [100, 0]), (2, 1, [1, 0]), (3, 1, [2, 0]), (4, 1, [3, 0]);

SELECT 'FINAL';
SELECT id, ver FROM tab FINAL ORDER BY L2Distance(vec, [0., 0.]) LIMIT 3;
SELECT 'setting final = 1';
SELECT id, ver FROM tab ORDER BY L2Distance(vec, [0., 0.]) LIMIT 3 SETTINGS final = 1;

-- A few candidates in a large granule.
CREATE TABLE tab_sparse (id UInt64, ver UInt64, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2) GRANULARITY 100000000)
ENGINE = ReplacingMergeTree(ver) ORDER BY id SETTINGS index_granularity = 8192, index_granularity_bytes = 10485760;
SYSTEM STOP MERGES tab_sparse;
-- id 1 at the reference vector, and 100 distant rows.
INSERT INTO tab_sparse SELECT if(number = 0, 1, number + 99), 1, [if(number = 0, 0, 1000 + number), 0] FROM numbers(101);
-- id 1 moved away, the three nearest rows, and 100 distant rows.
INSERT INTO tab_sparse SELECT if(number < 4, number + 1, number + 196), if(number = 0, 2, 1), [multiIf(number = 0, 100, number < 4, number, 2000 + number), 0] FROM numbers(104);

SELECT 'FINAL, few candidates in a granule';
SELECT id, ver FROM tab_sparse FINAL ORDER BY L2Distance(vec, [0., 0.]) LIMIT 3;

CREATE TABLE tab_deleted (id UInt64, ver UInt64, is_deleted UInt8, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2) GRANULARITY 100000000)
ENGINE = ReplacingMergeTree(ver, is_deleted) ORDER BY id SETTINGS index_granularity = 4;
SYSTEM STOP MERGES tab_deleted;
INSERT INTO tab_deleted VALUES (1, 1, 0, [0, 0]), (10, 1, 0, [50, 0]), (11, 1, 0, [60, 0]), (12, 1, 0, [70, 0]);
INSERT INTO tab_deleted VALUES (1, 2, 1, [100, 0]), (2, 1, 0, [1, 0]), (3, 1, 0, [2, 0]), (4, 1, 0, [3, 0]);

SELECT 'FINAL, deleted row';
SELECT id FROM tab_deleted FINAL ORDER BY L2Distance(vec, [0., 0.]) LIMIT 3;

DROP TABLE tab;
DROP TABLE tab_sparse;
DROP TABLE tab_deleted;
