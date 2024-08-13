-- Tags: no-fasttest, no-ordinary-database

-- Tests various bugs and special cases for vector indexes.

SET allow_experimental_vector_similarity_index = 1;
SET enable_analyzer = 1; -- 0 vs. 1 produce slightly different error codes, make it future-proof

DROP TABLE IF EXISTS tab;

SELECT 'Issue #52258: Empty Arrays or Arrays with default values are rejected';

CREATE TABLE tab (id UInt64, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance')) ENGINE = MergeTree() ORDER BY id;
INSERT INTO tab VALUES (1, []); -- { serverError INCORRECT_DATA }
INSERT INTO tab (id) VALUES (1); -- { serverError INCORRECT_DATA }
DROP TABLE tab;

SELECT 'It is possible to create parts with different Array vector sizes but there will be an error at query time';

CREATE TABLE tab(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance')) ENGINE = MergeTree ORDER BY id;
SYSTEM STOP MERGES tab;
INSERT INTO tab values (0, [2.2, 2.3]) (1, [3.1, 3.2]);
INSERT INTO tab values (2, [2.2, 2.3, 2.4]) (3, [3.1, 3.2, 3.3]);

WITH [0.0, 2.0] AS reference_vec
SELECT id, vec, L2Distance(vec, reference_vec)
FROM tab
ORDER BY L2Distance(vec, reference_vec)
LIMIT 3; -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }

DROP TABLE tab;

SELECT 'Correctness of index with > 1 mark';

CREATE TABLE tab(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance')) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity_bytes = 0, min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0, index_granularity = 8192; -- disable adaptive granularity due to bug
INSERT INTO tab SELECT number, [toFloat32(number), 0.0] from numbers(10000);

WITH [1.0, 0.0] AS reference_vec
SELECT id, vec, L2Distance(vec, reference_vec)
FROM tab
ORDER BY L2Distance(vec, reference_vec)
LIMIT 1;

WITH [9000.0, 0.0] AS reference_vec
SELECT id, vec, L2Distance(vec, reference_vec)
FROM tab
ORDER BY L2Distance(vec, reference_vec)
LIMIT 1;

DROP TABLE tab;
