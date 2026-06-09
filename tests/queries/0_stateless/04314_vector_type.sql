-- Tests the Vector(T, N) data type: a fixed-dimension dense vector stored contiguously (FLAT layout).

DROP TABLE IF EXISTS t_vector;
DROP TABLE IF EXISTS t_array;

SELECT '-- type name and DDL';
CREATE TABLE t_vector (id UInt32, v Vector(Float32, 4)) ENGINE = MergeTree ORDER BY id;
SELECT toTypeName(v) FROM t_vector LIMIT 1;
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't_vector' AND name = 'v';

SELECT '-- insert and read back (round-trip)';
INSERT INTO t_vector VALUES (1, [1, 2, 3, 4]), (2, [5, 6, 7, 8]);
SELECT id, v FROM t_vector ORDER BY id;

SELECT '-- distance functions vs a constant reference vector';
SELECT
    id,
    round(L2Distance(v, [0, 0, 0, 0]), 4) AS l2,
    round(L2SquaredDistance(v, [0, 0, 0, 0]), 4) AS l2sq,
    round(L1Distance(v, [0, 0, 0, 0]), 4) AS l1,
    round(cosineDistance(v, [1, 0, 0, 0]), 4) AS cos
FROM t_vector
ORDER BY id;

SELECT '-- distances match the equivalent Array(Float32) column';
CREATE TABLE t_array (id UInt32, v Array(Float32)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_array VALUES (1, [1, 2, 3, 4]), (2, [5, 6, 7, 8]);
SELECT
    v.id,
    round(L2Distance(v.v, [9, 8, 7, 6]), 4) = round(L2Distance(a.v, [9, 8, 7, 6]), 4) AS l2_eq,
    round(cosineDistance(v.v, [9, 8, 7, 6]), 4) = round(cosineDistance(a.v, [9, 8, 7, 6]), 4) AS cos_eq
FROM t_vector AS v
INNER JOIN t_array AS a USING (id)
ORDER BY v.id;

SELECT '-- CAST Array -> Vector and Vector -> Vector';
SELECT round(L2Distance(CAST([3, 4, 0, 0] AS Vector(Float32, 4)), [0, 0, 0, 0]), 4);
SELECT toTypeName(CAST([1, 2, 3, 4] AS Vector(Float32, 4)));

SELECT '-- dimension is enforced on CAST';
SELECT CAST([1, 2, 3] AS Vector(Float32, 4)); -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }

SELECT '-- Vector(Float64)';
DROP TABLE IF EXISTS t_vector64;
CREATE TABLE t_vector64 (id UInt32, v Vector(Float64, 3)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_vector64 VALUES (1, [1.5, 2.5, 3.5]);
SELECT v, round(L2Distance(v, [0, 0, 0]), 4) FROM t_vector64 ORDER BY id;

SELECT '-- Vector(BFloat16)';
DROP TABLE IF EXISTS t_vector_bf16;
CREATE TABLE t_vector_bf16 (id UInt32, v Vector(BFloat16, 4)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_vector_bf16 VALUES (1, [1, 2, 3, 4]);
SELECT v, round(L2Distance(v, [0, 0, 0, 0]), 2), round(cosineDistance(v, [1, 0, 0, 0]), 2) FROM t_vector_bf16 ORDER BY id;

DROP TABLE t_vector;
DROP TABLE t_array;
DROP TABLE t_vector64;
DROP TABLE t_vector_bf16;
