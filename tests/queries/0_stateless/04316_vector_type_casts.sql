-- Tests the CAST matrix of the Vector(T, N) data type.

SET allow_experimental_vector_type = 1;

SELECT '-- Array -> Vector';
SELECT CAST([1, 2, 3, 4] AS Vector(Float32, 4)) AS v, toTypeName(v);
SELECT CAST([1.5, 2.5] AS Vector(Float64, 2)) AS v, toTypeName(v);
SELECT CAST(materialize([1, 2, 3]) AS Vector(Float32, 3));
SELECT CAST([1, 2, 3] AS Vector(Float32, 4)); -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }
SELECT CAST([] AS Vector(Float32, 4)); -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }

SELECT '-- Vector -> Array';
SELECT CAST(CAST([1, 2, 3] AS Vector(Float32, 3)) AS Array(Float32)) AS a, toTypeName(a);
SELECT CAST(CAST([1, 2, 3] AS Vector(Float32, 3)) AS Array(Float64)) AS a, toTypeName(a);
SELECT CAST(CAST([1, 2, 3] AS Vector(Float32, 3)) AS Array(UInt8)) AS a, toTypeName(a);
SELECT arraySum(CAST(CAST([1, 2, 3] AS Vector(Float32, 3)) AS Array(Float32)));

SELECT '-- Vector -> Vector with a different element type';
SELECT CAST(CAST([1, 2, 3] AS Vector(Float32, 3)) AS Vector(Float64, 3)) AS v, toTypeName(v);
SELECT CAST(CAST([1, 2, 3] AS Vector(Float64, 3)) AS Vector(BFloat16, 3)) AS v, toTypeName(v);
SELECT CAST(CAST([1, 2, 3] AS Vector(Float32, 3)) AS Vector(Float32, 4)); -- { serverError TYPE_MISMATCH }

SELECT '-- String -> Vector';
SELECT CAST('[1, 2, 3]' AS Vector(Float32, 3)) AS v, toTypeName(v);

SELECT '-- accurateCastOrNull';
SELECT accurateCastOrNull([1, 2, 3], 'Vector(Float32, 3)') AS v, toTypeName(v);
SELECT accurateCastOrNull(42, 'Vector(Float32, 3)') AS v, toTypeName(v);

SELECT '-- Nullable(Vector)';
DROP TABLE IF EXISTS t_vector_nullable;
CREATE TABLE t_vector_nullable (id UInt32, v Nullable(Vector(Float32, 3))) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_vector_nullable VALUES (1, [1, 2, 3]), (2, NULL);
SELECT id, v FROM t_vector_nullable ORDER BY id;
SELECT CAST(NULL AS Nullable(Vector(Float32, 3)));
DROP TABLE t_vector_nullable;

SELECT '-- unsupported sources';
SELECT CAST(42 AS Vector(Float32, 3)); -- { serverError TYPE_MISMATCH }
SELECT CAST((1, 2, 3) AS Vector(Float32, 3)); -- { serverError TYPE_MISMATCH }
