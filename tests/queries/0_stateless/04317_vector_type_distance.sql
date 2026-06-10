-- Tests distance functions over the Vector(T, N) data type.

SET allow_experimental_vector_type = 1;

DROP TABLE IF EXISTS t_vector_dist;
CREATE TABLE t_vector_dist (id UInt32, v32 Vector(Float32, 3), v64 Vector(Float64, 3), vbf Vector(BFloat16, 3)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_vector_dist VALUES (1, [3, 4, 0], [3, 4, 0], [3, 4, 0]), (2, [1, 0, 0], [1, 0, 0], [1, 0, 0]);

SELECT '-- Vector against a constant Array reference';
SELECT id, round(L2Distance(v32, [0, 0, 0]), 4), round(L2SquaredDistance(v32, [0, 0, 0]), 4), round(L1Distance(v32, [0, 0, 0]), 4), round(LinfDistance(v32, [0, 0, 0]), 4), round(cosineDistance(v32, [1, 0, 0]), 4) FROM t_vector_dist ORDER BY id;
SELECT id, round(LpDistance(v32, [0, 0, 0], 3), 4) FROM t_vector_dist ORDER BY id;

SELECT '-- the Array argument may come first';
SELECT id, round(L2Distance([0, 0, 0], v32), 4) FROM t_vector_dist ORDER BY id;

SELECT '-- two Vector columns of the same type';
SELECT id, round(L2Distance(v32, v32), 4), round(L2Distance(v64, v64), 4) FROM t_vector_dist ORDER BY id;

SELECT '-- mixed element types are converted to the common type, not reinterpreted';
SELECT id, round(L2Distance(v32, v64), 4) FROM t_vector_dist ORDER BY id;
SELECT id, round(L2Distance(v64, v32), 4) FROM t_vector_dist ORDER BY id;
SELECT round(L2Distance(CAST([1, 2, 3] AS Vector(Float32, 3)), CAST([1, 2, 3] AS Vector(Float64, 3))), 4);

SELECT '-- BFloat16 vectors on both sides';
SELECT id, round(L2Distance(vbf, vbf), 2), round(cosineDistance(vbf, [1, 0, 0]), 2) FROM t_vector_dist ORDER BY id;

SELECT '-- dimension mismatch is rejected';
SELECT L2Distance(CAST([1, 2, 3] AS Vector(Float32, 3)), CAST([1, 2, 3, 4] AS Vector(Float32, 4))); -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }
SELECT L2Distance(v32, [1, 2]) FROM t_vector_dist; -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }

SELECT '-- results match the equivalent Array computation';
SELECT id,
    round(L2Distance(v32, [9, 8, 7]), 4) = round(L2Distance(CAST(v32 AS Array(Float32)), [9, 8, 7]), 4),
    round(cosineDistance(v32, [9, 8, 7]), 4) = round(cosineDistance(CAST(v32 AS Array(Float32)), [9, 8, 7]), 4)
FROM t_vector_dist ORDER BY id;

DROP TABLE t_vector_dist;
