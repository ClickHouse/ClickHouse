-- Tags: no-fasttest, no-ordinary-database

-- Tests various bugs and special cases for vector indexes.

SET allow_experimental_vector_similarity_index = 1;
SET enable_analyzer = 1; -- 0 vs. 1 produce slightly different error codes, make it future-proof

DROP TABLE IF EXISTS tab;

SELECT 'Rejects INSERTs of Arrays with different sizes';

CREATE TABLE tab(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance')) ENGINE = MergeTree ORDER BY id;
INSERT INTO tab values (0, [2.2, 2.3]) (1, [3.1, 3.2, 3.3]); -- { serverError INCORRECT_DATA }
DROP TABLE tab;

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

CREATE TABLE tab(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance')) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192;
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

SELECT 'Issue #69085: Reference vector computed by a subquery';

CREATE TABLE tab(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'cosineDistance', 'f16', 0, 0) GRANULARITY 2) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 3;
INSERT INTO tab VALUES (0, [4.6, 2.3]), (1, [2.0, 3.2]), (2, [4.2, 3.4]), (3, [5.3, 2.9]), (4, [2.4, 5.2]), (5, [5.3, 2.3]), (6, [1.0, 9.3]), (7, [5.5, 4.7]), (8, [6.4, 3.5]), (9, [5.3, 2.5]), (10, [6.4, 3.4]), (11, [6.4, 3.2]);

-- works
EXPLAIN indexes = 1
WITH [0., 2.] AS reference_vec
SELECT
    id,
    vec,
    cosineDistance(vec, reference_vec) AS distance
FROM tab
ORDER BY distance
LIMIT 1
SETTINGS enable_analyzer = 0;

-- does not work
EXPLAIN indexes = 1
WITH (
    SELECT vec
    FROM tab
    LIMIT 1
) AS reference_vec
SELECT
    id,
    vec,
    cosineDistance(vec, reference_vec) AS distance
FROM tab
ORDER BY distance
LIMIT 1
SETTINGS enable_analyzer = 0;

-- does not work as well
EXPLAIN indexes = 1
WITH (
    SELECT [0., 2.]
) AS reference_vec
SELECT
    id,
    vec,
    cosineDistance(vec, reference_vec) AS distance
FROM tab
ORDER BY distance
LIMIT 1
SETTINGS enable_analyzer = 0;

DROP TABLE tab;

SELECT 'index_granularity_bytes = 0 is disallowed';

-- If adaptive index granularity is disabled, certain vector search queries with PREWHERE run into LOGICAL_ERRORs.
--     SET allow_experimental_vector_similarity_index = 1;
--     CREATE TABLE tab (`id` Int32, `vec` Array(Float32), INDEX idx vec TYPE  vector_similarity('hnsw', 'L2Distance') GRANULARITY 100000000) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity_bytes = 0;
--     INSERT INTO tab SELECT number, [toFloat32(number), 0.] FROM numbers(10000);
--     WITH [1., 0.] AS reference_vec SELECT id, L2Distance(vec, reference_vec) FROM tab PREWHERE toLowCardinality(10) ORDER BY L2Distance(vec, reference_vec) ASC LIMIT 100;
-- As a workaround, force enabled adaptive index granularity for now (it is the default anyways).
CREATE TABLE tab(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance')) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity_bytes = 0; -- { serverError INVALID_SETTING_VALUE }

CREATE TABLE tab(id Int32, vec Array(Float32)) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity_bytes = 0;
ALTER TABLE tab ADD INDEX vec_idx1(vec) TYPE vector_similarity('hnsw', 'cosineDistance'); -- { serverError INVALID_SETTING_VALUE }

DROP TABLE tab;
