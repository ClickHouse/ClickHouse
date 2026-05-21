-- Tags: no-fasttest, no-ordinary-database

-- Comprehensive tests for MergeTree vector_spann secondary index.

SET allow_experimental_vector_spann_index = 1;
SET enable_analyzer = 1;
SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_max_limit_for_lazy_materialization = 10000;
SET hnsw_candidate_list_size_for_search = 100;

-- Tests 1-3: small index_granularity, centroid_ratio = 1.0, compare ANN vs brute force by row id only.
-- Test 4 (`dotProduct`): validate `vector_spann` result cardinality and DESC score ordering only.

SELECT '1. L2Distance';
DROP TABLE IF EXISTS tab_spann_l2_3;

CREATE TABLE tab_spann_l2_3 (id Int32, vec Array(Float32), INDEX idx vec TYPE vector_spann('spann', 'L2Distance', 2, 'bf16', 32, 128, 1.0))
    ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 2;

INSERT INTO tab_spann_l2_3 VALUES
    (0, [1.0, 0.0]), (1, [1.1, 0.0]), (2, [1.2, 0.0]), (3, [1.3, 0.0]), (4, [1.4, 0.0]),
    (5, [0.0, 2.0]), (6, [0.0, 2.1]), (7, [0.0, 2.2]), (8, [0.0, 2.3]), (9, [0.0, 2.4]);

SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    WITH [0.0, 2.0] AS reference_vec
    SELECT id
    FROM tab_spann_l2_3
    ORDER BY L2Distance(vec, reference_vec) ASC
    LIMIT 3
    SETTINGS use_skip_indexes = 1
)
WHERE explain LIKE '%Name: idx%'
    OR explain LIKE '%Description: vector_spann%';

SELECT count() FROM (
    WITH [0.0, 2.0] AS reference_vec
    SELECT id
    FROM tab_spann_l2_3
    ORDER BY L2Distance(vec, reference_vec) ASC
    LIMIT 3
    SETTINGS use_skip_indexes = 1
    EXCEPT
    WITH [0.0, 2.0] AS reference_vec
    SELECT id
    FROM tab_spann_l2_3
    ORDER BY L2Distance(vec, reference_vec) ASC
    LIMIT 3
    SETTINGS use_skip_indexes = 0
);

SELECT count() FROM (
    WITH [0.0, 2.0] AS reference_vec
    SELECT id
    FROM tab_spann_l2_3
    ORDER BY L2Distance(vec, reference_vec) ASC
    LIMIT 3
    SETTINGS use_skip_indexes = 0
    EXCEPT
    WITH [0.0, 2.0] AS reference_vec
    SELECT id
    FROM tab_spann_l2_3
    ORDER BY L2Distance(vec, reference_vec) ASC
    LIMIT 3
    SETTINGS use_skip_indexes = 1
);

DROP TABLE tab_spann_l2_3;

SELECT '2. L2Distance, bf16 quantization';
DROP TABLE IF EXISTS tab_spann_l2_7;

CREATE TABLE tab_spann_l2_7 (id Int32, vec Array(Float32), INDEX idx vec TYPE vector_spann('spann', 'L2Distance', 2, 'bf16', 32, 128, 1.0))
    ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 2;

INSERT INTO tab_spann_l2_7 VALUES
    (0, [1.0, 0.0]), (1, [1.1, 0.0]), (2, [1.2, 0.0]), (3, [1.3, 0.0]), (4, [1.4, 0.0]),
    (5, [0.0, 2.0]), (6, [0.0, 2.1]), (7, [0.0, 2.2]), (8, [0.0, 2.3]), (9, [0.0, 2.4]);

SELECT count() FROM (
    WITH [0.0, 2.0] AS reference_vec
    SELECT id
    FROM tab_spann_l2_7
    ORDER BY L2Distance(vec, reference_vec) ASC
    LIMIT 3
    SETTINGS use_skip_indexes = 1
    EXCEPT
    WITH [0.0, 2.0] AS reference_vec
    SELECT id
    FROM tab_spann_l2_7
    ORDER BY L2Distance(vec, reference_vec) ASC
    LIMIT 3
    SETTINGS use_skip_indexes = 0
);

SELECT count() FROM (
    WITH [0.0, 2.0] AS reference_vec
    SELECT id
    FROM tab_spann_l2_7
    ORDER BY L2Distance(vec, reference_vec) ASC
    LIMIT 3
    SETTINGS use_skip_indexes = 0
    EXCEPT
    WITH [0.0, 2.0] AS reference_vec
    SELECT id
    FROM tab_spann_l2_7
    ORDER BY L2Distance(vec, reference_vec) ASC
    LIMIT 3
    SETTINGS use_skip_indexes = 1
);

DROP TABLE tab_spann_l2_7;

SELECT '3. cosineDistance';
DROP TABLE IF EXISTS tab_spann_cos;

CREATE TABLE tab_spann_cos (id Int32, vec Array(Float32), INDEX idx vec TYPE vector_spann('spann', 'cosineDistance', 2, 'bf16', 32, 128, 1.0))
    ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 2;

INSERT INTO tab_spann_cos VALUES
    (0, [4.6, 2.3]), (1, [2.0, 3.2]), (2, [4.2, 3.4]), (3, [5.3, 2.9]), (4, [2.4, 5.2]), (5, [5.3, 2.3]),
    (6, [1.0, 9.3]), (7, [5.5, 4.7]), (8, [6.4, 3.5]), (9, [5.3, 2.5]), (10, [6.4, 3.4]), (11, [6.4, 3.2]);

SELECT count() FROM (
    WITH [0.0, 2.0] AS reference_vec
    SELECT id
    FROM tab_spann_cos
    ORDER BY cosineDistance(vec, reference_vec) ASC
    LIMIT 3
    SETTINGS use_skip_indexes = 1
    EXCEPT
    WITH [0.0, 2.0] AS reference_vec
    SELECT id
    FROM tab_spann_cos
    ORDER BY cosineDistance(vec, reference_vec) ASC
    LIMIT 3
    SETTINGS use_skip_indexes = 0
);

SELECT count() FROM (
    WITH [0.0, 2.0] AS reference_vec
    SELECT id
    FROM tab_spann_cos
    ORDER BY cosineDistance(vec, reference_vec) ASC
    LIMIT 3
    SETTINGS use_skip_indexes = 0
    EXCEPT
    WITH [0.0, 2.0] AS reference_vec
    SELECT id
    FROM tab_spann_cos
    ORDER BY cosineDistance(vec, reference_vec) ASC
    LIMIT 3
    SETTINGS use_skip_indexes = 1
);

DROP TABLE tab_spann_cos;

SELECT '4. dotProduct';
DROP TABLE IF EXISTS tab_spann_dot;

CREATE TABLE tab_spann_dot (id Int32, vec Array(Float32), INDEX idx vec TYPE vector_spann('spann', 'dotProduct', 2, 'bf16', 32, 128, 1.0))
    ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 2;

INSERT INTO tab_spann_dot VALUES
    (0, [4.6, 2.3]), (1, [2.0, 3.2]), (2, [4.2, 3.4]), (3, [5.3, 2.9]), (4, [2.4, 5.2]), (5, [5.3, 2.3]),
    (6, [1.0, 9.3]), (7, [5.5, 4.7]), (8, [6.4, 3.5]), (9, [5.3, 2.5]), (10, [6.4, 3.4]), (11, [6.4, 3.2]);

SELECT count() FROM (
    WITH [0.0, 2.0] AS reference_vec
    SELECT id
    FROM tab_spann_dot
    ORDER BY dotProduct(vec, reference_vec) DESC
    LIMIT 3
    SETTINGS use_skip_indexes = 1
);

SELECT arrayCount(x -> x, arrayMap((a, b) -> a < b, arrayPopBack(scores), arrayPopFront(scores)))
FROM (
    SELECT groupArray(score) AS scores
    FROM (
        WITH [0.0, 2.0] AS reference_vec
        SELECT dotProduct(vec, reference_vec) AS score
        FROM tab_spann_dot
        ORDER BY score DESC
        LIMIT 3
        SETTINGS use_skip_indexes = 1
    )
);

DROP TABLE tab_spann_dot;

SELECT '5. NaN rejected at insert';
DROP TABLE IF EXISTS tab_spann_nan;

CREATE TABLE tab_spann_nan (id Int32, vec Array(Float32), INDEX idx vec TYPE vector_spann('spann', 'L2Distance', 2))
    ENGINE = MergeTree ORDER BY id;

INSERT INTO tab_spann_nan VALUES (1, [toFloat32(nan), toFloat32(1.0)]); -- { serverError INCORRECT_DATA }

DROP TABLE tab_spann_nan;

SELECT '6. Inf rejected at insert';
DROP TABLE IF EXISTS tab_spann_inf;

CREATE TABLE tab_spann_inf (id Int32, vec Array(Float32), INDEX idx vec TYPE vector_spann('spann', 'L2Distance', 2))
    ENGINE = MergeTree ORDER BY id;

INSERT INTO tab_spann_inf VALUES (1, [toFloat32(+inf), toFloat32(1.0)]); -- { serverError INCORRECT_DATA }
INSERT INTO tab_spann_inf VALUES (1, [toFloat32(-inf), toFloat32(1.0)]); -- { serverError INCORRECT_DATA }

DROP TABLE tab_spann_inf;

SELECT '7. b1 quantization rejected';
CREATE TABLE tab_spann_b1 (vec Array(Float32), INDEX idx vec TYPE vector_spann('spann', 'cosineDistance', 8, 'b1', 32, 128, 0.5)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError INCORRECT_DATA }

SELECT '8. invalid centroid_ratio';
CREATE TABLE tab_spann_ratio_0 (vec Array(Float32), INDEX idx vec TYPE vector_spann('spann', 'L2Distance', 2, 'bf16', 32, 128, 0.0)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError INCORRECT_DATA }
CREATE TABLE tab_spann_ratio_15 (vec Array(Float32), INDEX idx vec TYPE vector_spann('spann', 'L2Distance', 2, 'bf16', 32, 128, 1.5)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError INCORRECT_DATA }

SELECT '9. dimension mismatch at query time';
DROP TABLE IF EXISTS tab_spann_dim;

CREATE TABLE tab_spann_dim (id Int32, vec Array(Float32), INDEX idx vec TYPE vector_spann('spann', 'L2Distance', 2))
    ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 2;

INSERT INTO tab_spann_dim VALUES (0, [1.0, 0.0]), (1, [0.0, 1.0]);

WITH [1.0, 0.0, 0.0] AS reference_vec
SELECT id
FROM tab_spann_dim
ORDER BY L2Distance(vec, reference_vec) ASC
LIMIT 1
SETTINGS use_skip_indexes = 1; -- { serverError INCORRECT_QUERY }

DROP TABLE tab_spann_dim;
