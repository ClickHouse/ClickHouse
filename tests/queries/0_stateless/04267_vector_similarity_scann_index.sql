-- Tags: no-fasttest, no-ordinary-database
-- Tests for MergeTree vector_similarity('scann', ...) secondary index.

SET enable_analyzer = 1;
SET vector_search_with_rescoring = 1;

-- Shared dataset for L2Distance tests (reference vector [0.0, 2.0]).
-- Nearest by L2: ids 5, 6, 7.
SELECT '1. L2Distance';
DROP TABLE IF EXISTS tab_scann_l2;
CREATE TABLE tab_scann_l2 (id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('scann', 'L2Distance', 2))
    ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192;
INSERT INTO tab_scann_l2 VALUES
    (0, [1.0, 0.0]), (1, [1.1, 0.0]), (2, [1.2, 0.0]), (3, [1.3, 0.0]), (4, [1.4, 0.0]),
    (5, [0.0, 2.0]), (6, [0.0, 2.1]), (7, [0.0, 2.2]), (8, [0.0, 2.3]), (9, [0.0, 2.4]);
WITH [0.0, 2.0] AS reference_vec
SELECT id, L2Distance(vec, reference_vec) AS dist
FROM tab_scann_l2
ORDER BY dist ASC
LIMIT 3
SETTINGS use_skip_indexes = 1;
SELECT count() FROM (
    WITH [0.0, 2.0] AS reference_vec
    SELECT id, L2Distance(vec, reference_vec) AS dist
    FROM tab_scann_l2
    ORDER BY dist ASC
    LIMIT 3
    SETTINGS use_skip_indexes = 1
    EXCEPT
    WITH [0.0, 2.0] AS reference_vec
    SELECT id, L2Distance(vec, reference_vec) AS dist
    FROM tab_scann_l2
    ORDER BY dist ASC
    LIMIT 3
    SETTINGS use_skip_indexes = 0
);
SELECT count() FROM (
    WITH [0.0, 2.0] AS reference_vec
    SELECT id, L2Distance(vec, reference_vec) AS dist
    FROM tab_scann_l2
    ORDER BY dist ASC
    LIMIT 3
    SETTINGS use_skip_indexes = 0
    EXCEPT
    WITH [0.0, 2.0] AS reference_vec
    SELECT id, L2Distance(vec, reference_vec) AS dist
    FROM tab_scann_l2
    ORDER BY dist ASC
    LIMIT 3
    SETTINGS use_skip_indexes = 1
);
DROP TABLE tab_scann_l2;

-- Shared dataset for cosineDistance / dotProduct (reference vector [0.0, 2.0]).
SELECT '2. cosineDistance';
DROP TABLE IF EXISTS tab_scann_cos;
CREATE TABLE tab_scann_cos (id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('scann', 'cosineDistance', 2))
    ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192;
INSERT INTO tab_scann_cos VALUES
    (0, [4.6, 2.3]), (1, [2.0, 3.2]), (2, [4.2, 3.4]), (3, [5.3, 2.9]), (4, [2.4, 5.2]), (5, [5.3, 2.3]),
    (6, [1.0, 9.3]), (7, [5.5, 4.7]), (8, [6.4, 3.5]), (9, [5.3, 2.5]), (10, [6.4, 3.4]), (11, [6.4, 3.2]);
WITH [0.0, 2.0] AS reference_vec
SELECT id, cosineDistance(vec, reference_vec) AS dist
FROM tab_scann_cos
ORDER BY dist ASC
LIMIT 3
SETTINGS use_skip_indexes = 1;
SELECT count() FROM (
    WITH [0.0, 2.0] AS reference_vec
    SELECT id, cosineDistance(vec, reference_vec) AS dist
    FROM tab_scann_cos
    ORDER BY dist ASC
    LIMIT 3
    SETTINGS use_skip_indexes = 1
    EXCEPT
    WITH [0.0, 2.0] AS reference_vec
    SELECT id, cosineDistance(vec, reference_vec) AS dist
    FROM tab_scann_cos
    ORDER BY dist ASC
    LIMIT 3
    SETTINGS use_skip_indexes = 0
);
SELECT count() FROM (
    WITH [0.0, 2.0] AS reference_vec
    SELECT id, cosineDistance(vec, reference_vec) AS dist
    FROM tab_scann_cos
    ORDER BY dist ASC
    LIMIT 3
    SETTINGS use_skip_indexes = 0
    EXCEPT
    WITH [0.0, 2.0] AS reference_vec
    SELECT id, cosineDistance(vec, reference_vec) AS dist
    FROM tab_scann_cos
    ORDER BY dist ASC
    LIMIT 3
    SETTINGS use_skip_indexes = 1
);
DROP TABLE tab_scann_cos;

SELECT '3. dotProduct';
DROP TABLE IF EXISTS tab_scann_dot;
CREATE TABLE tab_scann_dot (id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('scann', 'dotProduct', 2))
    ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192;
INSERT INTO tab_scann_dot VALUES
    (0, [4.6, 2.3]), (1, [2.0, 3.2]), (2, [4.2, 3.4]), (3, [5.3, 2.9]), (4, [2.4, 5.2]), (5, [5.3, 2.3]),
    (6, [1.0, 9.3]), (7, [5.5, 4.7]), (8, [6.4, 3.5]), (9, [5.3, 2.5]), (10, [6.4, 3.4]), (11, [6.4, 3.2]);
WITH [0.0, 2.0] AS reference_vec
SELECT id, dotProduct(vec, reference_vec) AS score
FROM tab_scann_dot
ORDER BY score DESC
LIMIT 3
SETTINGS use_skip_indexes = 1;
SELECT count() FROM (
    WITH [0.0, 2.0] AS reference_vec
    SELECT id, dotProduct(vec, reference_vec) AS score
    FROM tab_scann_dot
    ORDER BY score DESC
    LIMIT 3
    SETTINGS use_skip_indexes = 1
    EXCEPT
    WITH [0.0, 2.0] AS reference_vec
    SELECT id, dotProduct(vec, reference_vec) AS score
    FROM tab_scann_dot
    ORDER BY score DESC
    LIMIT 3
    SETTINGS use_skip_indexes = 0
);
SELECT count() FROM (
    WITH [0.0, 2.0] AS reference_vec
    SELECT id, dotProduct(vec, reference_vec) AS score
    FROM tab_scann_dot
    ORDER BY score DESC
    LIMIT 3
    SETTINGS use_skip_indexes = 0
    EXCEPT
    WITH [0.0, 2.0] AS reference_vec
    SELECT id, dotProduct(vec, reference_vec) AS score
    FROM tab_scann_dot
    ORDER BY score DESC
    LIMIT 3
    SETTINGS use_skip_indexes = 1
);
DROP TABLE tab_scann_dot;

SELECT '4. invalid distance function';
CREATE TABLE tab_scann_bad_dist (vec Array(Float32), INDEX idx vec TYPE vector_similarity('scann', 'hamming', 2)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError INCORRECT_DATA }

SELECT '5. too few arguments';
CREATE TABLE tab_scann_too_few (vec Array(Float32), INDEX idx vec TYPE vector_similarity('scann', 'L2Distance')) ENGINE = MergeTree ORDER BY tuple(); -- { serverError INCORRECT_QUERY }

SELECT '6. too many arguments';
CREATE TABLE tab_scann_too_many (vec Array(Float32), INDEX idx vec TYPE vector_similarity('scann', 'L2Distance', 2, 'extra')) ENGINE = MergeTree ORDER BY tuple(); -- { serverError INCORRECT_QUERY }

SELECT '7. dimensions must be > 0';
CREATE TABLE tab_scann_zero_dim (vec Array(Float32), INDEX idx vec TYPE vector_similarity('scann', 'L2Distance', 0)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError INCORRECT_DATA }

SELECT '8. must be created on Array(Float32) or Array(Float64) column';
CREATE TABLE tab_scann_wrong_col (vec Array(UInt64), INDEX idx vec TYPE vector_similarity('scann', 'L2Distance', 2)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError ILLEGAL_COLUMN }

SELECT '9. dimension mismatch at insert';
DROP TABLE IF EXISTS tab_scann_dim_ins;
CREATE TABLE tab_scann_dim_ins (id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('scann', 'L2Distance', 2))
    ENGINE = MergeTree ORDER BY id;
INSERT INTO tab_scann_dim_ins VALUES (0, [1.0, 2.0, 3.0]); -- { serverError INCORRECT_DATA }
DROP TABLE tab_scann_dim_ins;
