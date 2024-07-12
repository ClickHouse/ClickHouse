-- Tags: no-fasttest, no-ubsan, no-cpu-aarch64, no-ordinary-database, no-asan

-- Tests vector search in ClickHouse, i.e. Annoy and Usearch indexes. Both index types share similarities in implementation and usage,
-- therefore they are tested in a single file.

-- This file contains tests for various bugs and special cases

SET allow_experimental_annoy_index = 1;
SET allow_experimental_usearch_index = 1;

SET enable_analyzer = 1; -- 0 vs. 1 produce slightly different error codes, make it future-proof

DROP TABLE IF EXISTS tab;

SELECT 'Issue #52258: Empty Arrays or Arrays with default values are rejected';

SELECT '- Annoy';

CREATE TABLE tab (id UInt64, vec Array(Float32), INDEX idx vec TYPE annoy()) ENGINE = MergeTree() ORDER BY (id);
INSERT INTO tab VALUES (1, []); -- { serverError INCORRECT_DATA }
INSERT INTO tab (id) VALUES (1); -- { serverError INCORRECT_DATA }
DROP TABLE tab;

CREATE TABLE tab (id UInt64, vec Tuple(Float32, Float32), INDEX idx vec TYPE annoy()) ENGINE = MergeTree() ORDER BY (id);
INSERT INTO tab (id) VALUES (1); -- works fine, takes on default tuple (0.0, 0.0)
DROP TABLE tab;

SELECT '- Usearch';

CREATE TABLE tab (id UInt64, vec Array(Float32), INDEX idx vec TYPE usearch()) ENGINE = MergeTree() ORDER BY (id);
INSERT INTO tab VALUES (1, []); -- { serverError INCORRECT_DATA }
INSERT INTO tab (id) VALUES (1); -- { serverError INCORRECT_DATA }
DROP TABLE tab;

CREATE TABLE tab (id UInt64, vec Tuple(Float32, Float32), INDEX idx vec TYPE usearch()) ENGINE = MergeTree() ORDER BY (id);
INSERT INTO tab (id) VALUES (1); -- works fine, takes on default tuple (0.0, 0.0)
DROP TABLE tab;

SELECT 'It is possible to create parts with different Array vector sizes but there will be an error at query time';

SELECT '- Annoy';

CREATE TABLE tab(id Int32, vec Array(Float32), INDEX idx vec TYPE annoy()) ENGINE = MergeTree ORDER BY id;
SYSTEM STOP MERGES tab;
INSERT INTO tab values (0, [2.2, 2.3]) (1, [3.1, 3.2]);
INSERT INTO tab values (2, [2.2, 2.3, 2.4]) (3, [3.1, 3.2, 3.3]);

WITH [0.0, 2.0] AS reference_vec
SELECT id, vec, L2Distance(vec, reference_vec)
FROM tab
ORDER BY L2Distance(vec, reference_vec)
LIMIT 3; -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }

DROP TABLE tab;

SELECT '- Usearch';

CREATE TABLE tab(id Int32, vec Array(Float32), INDEX idx vec TYPE usearch()) ENGINE = MergeTree ORDER BY id;
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

SELECT '- Annoy';

CREATE TABLE tab(id Int32, vec Array(Float32), INDEX idx vec TYPE annoy()) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity_bytes=0, min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0, index_granularity=8192; -- disable adaptive granularity due to bug
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

-- same, but with Tuples
CREATE TABLE tab(id Int32, vec Tuple(Float32, Float32), INDEX idx vec TYPE annoy()) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity_bytes=0, min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0, index_granularity=8192; -- disable adaptive granularity due to bug
INSERT INTO tab SELECT number, (toFloat32(number), 0.0) from numbers(10000);

WITH (1.0, 0.0) AS reference_vec
SELECT id, vec, L2Distance(vec, reference_vec)
FROM tab
ORDER BY L2Distance(vec, reference_vec)
LIMIT 1;

WITH (9000.0, 0.0) AS reference_vec
SELECT id, vec, L2Distance(vec, reference_vec)
FROM tab
ORDER BY L2Distance(vec, reference_vec)
LIMIT 1;

DROP TABLE tab;

SELECT '- Usearch';

CREATE TABLE tab(id Int32, vec Array(Float32), INDEX idx vec TYPE usearch()) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity_bytes=0, min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0, index_granularity=8192; -- disable adaptive granularity due to bug
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

-- same, but with Tuples
CREATE TABLE tab(id Int32, vec Tuple(Float32, Float32), INDEX idx vec TYPE usearch()) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity_bytes=0, min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0, index_granularity=8192; -- disable adaptive granularity due to bug
INSERT INTO tab SELECT number, (toFloat32(number), 0.0) from numbers(10000);

WITH (1.0, 0.0) AS reference_vec
SELECT id, vec, L2Distance(vec, reference_vec)
FROM tab
ORDER BY L2Distance(vec, reference_vec)
LIMIT 1;

WITH (9000.0, 0.0) AS reference_vec
SELECT id, vec, L2Distance(vec, reference_vec)
FROM tab
ORDER BY L2Distance(vec, reference_vec)
LIMIT 1;

DROP TABLE tab;
