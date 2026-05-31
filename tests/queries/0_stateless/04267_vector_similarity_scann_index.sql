-- Tags: no-fasttest, no-ordinary-database
-- Tests for MergeTree vector_similarity('scann', ...) secondary index.
-- Tests 1-3 insert >= 2000 vectors so the ScaNN index is actually built.
-- The original reference rows are padded with distant vectors so that the
-- expected top-K is unchanged while the ANN path is exercised.

SET enable_analyzer = 1;
SET vector_search_with_rescoring = 1;

SELECT '1. L2Distance';
DROP TABLE IF EXISTS tab_scann_l2;
CREATE TABLE tab_scann_l2 (id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('scann', 'L2Distance', 2))
    ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192;
INSERT INTO tab_scann_l2 VALUES
    (0, [1.0, 0.0]), (1, [1.1, 0.0]), (2, [1.2, 0.0]), (3, [1.3, 0.0]), (4, [1.4, 0.0]),
    (5, [0.0, 2.0]), (6, [0.0, 2.1]), (7, [0.0, 2.2]), (8, [0.0, 2.3]), (9, [0.0, 2.4]);
INSERT INTO tab_scann_l2
    SELECT 10 + toInt32(number), [toFloat32((number + 1) * 100.0), toFloat32(0.0)]
    FROM numbers(1990);
OPTIMIZE TABLE tab_scann_l2 FINAL;
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
-- Verify the optimize_plan=true path (rescoring=0) returns the correct top-3 IDs.
SELECT count() FROM (
    WITH [0.0, 2.0] AS reference_vec
    SELECT id FROM tab_scann_l2 ORDER BY L2Distance(vec, reference_vec) ASC LIMIT 3
    SETTINGS vector_search_with_rescoring = 0, use_skip_indexes = 1
    EXCEPT
    WITH [0.0, 2.0] AS reference_vec
    SELECT id FROM tab_scann_l2 ORDER BY L2Distance(vec, reference_vec) ASC LIMIT 3
    SETTINGS use_skip_indexes = 0
);
DROP TABLE tab_scann_l2;

SELECT '2. cosineDistance';
DROP TABLE IF EXISTS tab_scann_cos;
CREATE TABLE tab_scann_cos (id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('scann', 'cosineDistance', 2))
    ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192;
INSERT INTO tab_scann_cos VALUES
    (0, [4.6, 2.3]), (1, [2.0, 3.2]), (2, [4.2, 3.4]), (3, [5.3, 2.9]), (4, [2.4, 5.2]), (5, [5.3, 2.3]),
    (6, [1.0, 9.3]), (7, [5.5, 4.7]), (8, [6.4, 3.5]), (9, [5.3, 2.5]), (10, [6.4, 3.4]), (11, [6.4, 3.2]);
INSERT INTO tab_scann_cos
    SELECT 12 + toInt32(number), [toFloat32((number + 1) * 100.0), toFloat32(0.0)]
    FROM numbers(1988);
OPTIMIZE TABLE tab_scann_cos FINAL;
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
-- Verify the optimize_plan=true path (rescoring=0).
SELECT count() FROM (
    WITH [0.0, 2.0] AS reference_vec
    SELECT id FROM tab_scann_cos ORDER BY cosineDistance(vec, reference_vec) ASC LIMIT 3
    SETTINGS vector_search_with_rescoring = 0, use_skip_indexes = 1
    EXCEPT
    WITH [0.0, 2.0] AS reference_vec
    SELECT id FROM tab_scann_cos ORDER BY cosineDistance(vec, reference_vec) ASC LIMIT 3
    SETTINGS use_skip_indexes = 0
);
DROP TABLE tab_scann_cos;

SELECT '3. dotProduct';
DROP TABLE IF EXISTS tab_scann_dot;
CREATE TABLE tab_scann_dot (id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('scann', 'dotProduct', 2))
    ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192;
INSERT INTO tab_scann_dot VALUES
    (0, [4.6, 2.3]), (1, [2.0, 3.2]), (2, [4.2, 3.4]), (3, [5.3, 2.9]), (4, [2.4, 5.2]), (5, [5.3, 2.3]),
    (6, [1.0, 9.3]), (7, [5.5, 4.7]), (8, [6.4, 3.5]), (9, [5.3, 2.5]), (10, [6.4, 3.4]), (11, [6.4, 3.2]);
INSERT INTO tab_scann_dot
    SELECT 12 + toInt32(number), [toFloat32((number + 1) * 100.0), toFloat32(0.0)]
    FROM numbers(1988);
OPTIMIZE TABLE tab_scann_dot FINAL;
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
-- Verify the optimize_plan=true path (rescoring=0).
SELECT count() FROM (
    WITH [0.0, 2.0] AS reference_vec
    SELECT id FROM tab_scann_dot ORDER BY dotProduct(vec, reference_vec) DESC LIMIT 3
    SETTINGS vector_search_with_rescoring = 0, use_skip_indexes = 1
    EXCEPT
    WITH [0.0, 2.0] AS reference_vec
    SELECT id FROM tab_scann_dot ORDER BY dotProduct(vec, reference_vec) DESC LIMIT 3
    SETTINGS use_skip_indexes = 0
);
DROP TABLE tab_scann_dot;

-- Test 4: fallback path (< 1000 vectors, ScaNN index not built).
-- Verifies that both rescoring modes produce correct results when the index is absent.
SELECT '4. Fallback path (< 1000 vectors)';
DROP TABLE IF EXISTS tab_scann_fallback;
CREATE TABLE tab_scann_fallback (id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('scann', 'L2Distance', 2))
    ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192;
INSERT INTO tab_scann_fallback VALUES
    (0, [1.0, 0.0]), (1, [1.1, 0.0]), (2, [1.2, 0.0]), (3, [1.3, 0.0]), (4, [1.4, 0.0]),
    (5, [0.0, 2.0]), (6, [0.0, 2.1]), (7, [0.0, 2.2]), (8, [0.0, 2.3]), (9, [0.0, 2.4]);
-- rescoring=1: fallback should return the same results as the exact search.
SELECT count() FROM (
    WITH [0.0, 2.0] AS reference_vec
    SELECT id FROM tab_scann_fallback ORDER BY L2Distance(vec, reference_vec) ASC LIMIT 3
    SETTINGS use_skip_indexes = 1
    EXCEPT
    WITH [0.0, 2.0] AS reference_vec
    SELECT id FROM tab_scann_fallback ORDER BY L2Distance(vec, reference_vec) ASC LIMIT 3
    SETTINGS use_skip_indexes = 0
);
-- rescoring=0: fallback must not set sentinel +inf distances (regression test for the
-- bug where distances.has_value()==true caused optimize_plan=true on fallback parts).
SELECT count() FROM (
    WITH [0.0, 2.0] AS reference_vec
    SELECT id FROM tab_scann_fallback ORDER BY L2Distance(vec, reference_vec) ASC LIMIT 3
    SETTINGS vector_search_with_rescoring = 0, use_skip_indexes = 1
    EXCEPT
    WITH [0.0, 2.0] AS reference_vec
    SELECT id FROM tab_scann_fallback ORDER BY L2Distance(vec, reference_vec) ASC LIMIT 3
    SETTINGS use_skip_indexes = 0
);
DROP TABLE tab_scann_fallback;

SELECT '5. invalid distance function';
CREATE TABLE tab_scann_bad_dist (vec Array(Float32), INDEX idx vec TYPE vector_similarity('scann', 'hamming', 2)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError INCORRECT_DATA }

SELECT '6. too few arguments';
CREATE TABLE tab_scann_too_few (vec Array(Float32), INDEX idx vec TYPE vector_similarity('scann', 'L2Distance')) ENGINE = MergeTree ORDER BY tuple(); -- { serverError INCORRECT_QUERY }

SELECT '7. too many arguments';
CREATE TABLE tab_scann_too_many (vec Array(Float32), INDEX idx vec TYPE vector_similarity('scann', 'L2Distance', 2, 'extra')) ENGINE = MergeTree ORDER BY tuple(); -- { serverError INCORRECT_QUERY }

SELECT '8. dimensions must be > 0';
CREATE TABLE tab_scann_zero_dim (vec Array(Float32), INDEX idx vec TYPE vector_similarity('scann', 'L2Distance', 0)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError INCORRECT_DATA }

SELECT '9. must be created on Array(Float32) or Array(Float64) column';
CREATE TABLE tab_scann_wrong_col (vec Array(UInt64), INDEX idx vec TYPE vector_similarity('scann', 'L2Distance', 2)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError ILLEGAL_COLUMN }

SELECT '10. dimension mismatch at insert';
DROP TABLE IF EXISTS tab_scann_dim_ins;
CREATE TABLE tab_scann_dim_ins (id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('scann', 'L2Distance', 2))
    ENGINE = MergeTree ORDER BY id;
INSERT INTO tab_scann_dim_ins VALUES (0, [1.0, 2.0, 3.0]); -- { serverError INCORRECT_DATA }
DROP TABLE tab_scann_dim_ins;

SELECT '12. non-finite values rejected at insert (NaN/Inf)';
DROP TABLE IF EXISTS tab_scann_nonfinite;
CREATE TABLE tab_scann_nonfinite (id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('scann', 'L2Distance', 2))
    ENGINE = MergeTree ORDER BY id;
INSERT INTO tab_scann_nonfinite VALUES (0, [nan, 1.0]); -- { serverError INCORRECT_DATA }
INSERT INTO tab_scann_nonfinite VALUES (0, [inf, 1.0]); -- { serverError INCORRECT_DATA }
INSERT INTO tab_scann_nonfinite VALUES (0, [-inf, 1.0]); -- { serverError INCORRECT_DATA }
DROP TABLE tab_scann_nonfinite;

SELECT '13. non-finite values rejected in query vector';
DROP TABLE IF EXISTS tab_scann_nonfinite_q;
CREATE TABLE tab_scann_nonfinite_q (id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('scann', 'L2Distance', 2))
    ENGINE = MergeTree ORDER BY id;
INSERT INTO tab_scann_nonfinite_q SELECT toInt32(number), [toFloat32(number), toFloat32(number + 1)] FROM numbers(2000);
OPTIMIZE TABLE tab_scann_nonfinite_q FINAL;
WITH [nan, 0.0] AS ref SELECT id FROM tab_scann_nonfinite_q ORDER BY L2Distance(vec, ref) LIMIT 1 SETTINGS use_skip_indexes = 1; -- { serverError INCORRECT_DATA }
WITH [inf, 0.0] AS ref SELECT id FROM tab_scann_nonfinite_q ORDER BY L2Distance(vec, ref) LIMIT 1 SETTINGS use_skip_indexes = 1; -- { serverError INCORRECT_DATA }
DROP TABLE tab_scann_nonfinite_q;

-- Test 14: index survives DETACH/ATTACH (serialization round-trip).
SELECT '14. Serialization round-trip (DETACH/ATTACH)';
DROP TABLE IF EXISTS tab_scann_detach;
CREATE TABLE tab_scann_detach (id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('scann', 'L2Distance', 2))
    ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192;
INSERT INTO tab_scann_detach VALUES
    (0, [1.0, 0.0]), (1, [1.1, 0.0]), (2, [1.2, 0.0]), (3, [1.3, 0.0]), (4, [1.4, 0.0]),
    (5, [0.0, 2.0]), (6, [0.0, 2.1]), (7, [0.0, 2.2]), (8, [0.0, 2.3]), (9, [0.0, 2.4]);
INSERT INTO tab_scann_detach
    SELECT 10 + toInt32(number), [toFloat32((number + 1) * 100.0), toFloat32(0.0)]
    FROM numbers(1990);
OPTIMIZE TABLE tab_scann_detach FINAL;
DETACH TABLE tab_scann_detach SYNC;
ATTACH TABLE tab_scann_detach;
WITH [0.0, 2.0] AS reference_vec
SELECT isFinite(L2Distance(vec, reference_vec))
FROM tab_scann_detach
ORDER BY L2Distance(vec, reference_vec) ASC
LIMIT 1
SETTINGS vector_search_with_rescoring = 0, use_skip_indexes = 1;
SELECT count() FROM (
    WITH [0.0, 2.0] AS reference_vec
    SELECT id FROM tab_scann_detach ORDER BY L2Distance(vec, reference_vec) ASC LIMIT 1
    SETTINGS vector_search_with_rescoring = 0, use_skip_indexes = 1
    EXCEPT
    WITH [0.0, 2.0] AS reference_vec
    SELECT id FROM tab_scann_detach ORDER BY L2Distance(vec, reference_vec) ASC LIMIT 1
    SETTINGS use_skip_indexes = 0
);
DROP TABLE tab_scann_detach;
