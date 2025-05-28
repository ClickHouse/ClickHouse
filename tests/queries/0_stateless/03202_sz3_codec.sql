-- Tags: no-fasttest
-- no-fasttest: needs sz3 library

DROP TABLE IF EXISTS codecTest;

SET allow_experimental_codecs = 1;
SET cross_to_inner_join_rewrite = 1;

CREATE TABLE codecTest (
    key      UInt64,
    name     String,
    ref_valueF64 Float64,
    ref_valueF32 Float32,
    valueF64 Float64  CODEC(SZ3),
    valueF32 Float32  CODEC(SZ3)
) Engine = MergeTree ORDER BY key;

-- best case - same value
INSERT INTO codecTest (key, name, ref_valueF64, ref_valueF32, valueF64, valueF32)
    SELECT number AS n, 'e()', e() AS v, v, v, v FROM system.numbers LIMIT 1, 100;

-- good case - values that grow insignificantly
INSERT INTO codecTest (key, name, ref_valueF64, ref_valueF32, valueF64, valueF32)
    SELECT number AS n, 'log2(n)', log2(n) AS v, v, v, v FROM system.numbers LIMIT 101, 100;

-- bad case - values differ significantly
INSERT INTO codecTest (key, name, ref_valueF64, ref_valueF32, valueF64, valueF32)
    SELECT number AS n, 'n*sqrt(n)', n*sqrt(n) AS v, v, v, v FROM system.numbers LIMIT 201, 100;

-- worst case - almost like a random values
INSERT INTO codecTest (key, name, ref_valueF64, ref_valueF32, valueF64, valueF32)
    SELECT number AS n, 'sin(n*n*n)*n', sin(n * n * n)*n AS v, v, v, v FROM system.numbers LIMIT 301, 100;



SELECT 'F64';
SELECT
    c1.key, c1.name,
    c1.ref_valueF64, c1.valueF64, c1.ref_valueF64 - c1.valueF64 AS dF64,
    'prev:',
    c2.key, c2.ref_valueF64
FROM
    codecTest as c1, codecTest as c2
WHERE
    abs(1 - dF64 / c1.ref_valueF64) < 0.01
AND
    c2.key = c1.key - 1
LIMIT 10;


SELECT 'F32';
SELECT
    c1.key, c1.name,
    c1.ref_valueF32, c1.valueF32, c1.ref_valueF32 - c1.valueF32 AS dF32,
    'prev:',
    c2.key, c2.ref_valueF32
FROM
    codecTest as c1, codecTest as c2
WHERE
    abs(1 - dF32 / c1.ref_valueF32) < 0.01
AND
    c2.key = c1.key - 1
LIMIT 10;

DROP TABLE codecTest;

-- just test flags

CREATE TABLE codecTest (
    key      UInt64,
    name     String,
    ref_valueF64 Float64,
    ref_valueF32 Float32,
    valueF64 Float64  CODEC(SZ3('ALGO_INTERP', 'REL', 0.01)),
    valueF32 Float32  CODEC(SZ3('ALGO_INTERP', 'REL', 0.01))
) Engine = MergeTree ORDER BY key;

-- best case - same value
INSERT INTO codecTest (key, name, ref_valueF64, ref_valueF32, valueF64, valueF32)
    SELECT number AS n, 'e()', e() AS v, v, v, v FROM system.numbers LIMIT 1, 100;

-- good case - values that grow insignificantly
INSERT INTO codecTest (key, name, ref_valueF64, ref_valueF32, valueF64, valueF32)
    SELECT number AS n, 'log2(n)', log2(n) AS v, v, v, v FROM system.numbers LIMIT 101, 100;

-- bad case - values differ significantly
INSERT INTO codecTest (key, name, ref_valueF64, ref_valueF32, valueF64, valueF32)
    SELECT number AS n, 'n*sqrt(n)', n*sqrt(n) AS v, v, v, v FROM system.numbers LIMIT 201, 100;

-- worst case - almost like a random values
INSERT INTO codecTest (key, name, ref_valueF64, ref_valueF32, valueF64, valueF32)
    SELECT number AS n, 'sin(n*n*n)*n', sin(n * n * n)*n AS v, v, v, v FROM system.numbers LIMIT 301, 100;


SELECT 'F64';
SELECT
    c1.key, c1.name,
    c1.ref_valueF64, c1.valueF64, c1.ref_valueF64 - c1.valueF64 AS dF64,
    'prev:',
    c2.key, c2.ref_valueF64
FROM
    codecTest as c1, codecTest as c2
WHERE
    abs(1 - dF64 / c1.ref_valueF64) < 0.01
AND
    c2.key = c1.key - 1
LIMIT 10;


SELECT 'F32';
SELECT
    c1.key, c1.name,
    c1.ref_valueF32, c1.valueF32, c1.ref_valueF32 - c1.valueF32 AS dF32,
    'prev:',
    c2.key, c2.ref_valueF32
FROM
    codecTest as c1, codecTest as c2
WHERE
    abs(1 - dF32 / c1.ref_valueF32) < 0.01
AND
    c2.key = c1.key - 1
LIMIT 10;

SELECT 'TEST 1D ARRAY';

DROP TABLE IF EXISTS codecArrayTest;

CREATE TABLE codecArrayTest (
    key        UInt64,
    name       String,
    ref_valueF64 Array(Float64),
    ref_valueF32 Array(Float32),
    valueF64   Array(Float64) CODEC(SZ3('ALGO_INTERP', 'REL', 0.01)),
    valueF32   Array(Float32) CODEC(SZ3('ALGO_INTERP', 'REL', 0.01))
) ENGINE = MergeTree ORDER BY key;

-- best case - all elements equal
INSERT INTO codecArrayTest
SELECT 
    number AS key,
    'constant array' AS name,
    arrayResize([e()], 10) AS v64,
    arrayResize([toFloat32(e())], 10) AS v32,
    v64, v32
FROM system.numbers LIMIT 10;

-- good case - slightly growing values
INSERT INTO codecArrayTest
SELECT 
    number + 100 AS key,
    'log(1..10)' AS name,
    arrayMap(i -> log(number + i + 1), range(100)) AS v64,
    arrayMap(i -> toFloat32(log(number + i + 1)), range(100)) AS v32,
    v64, v32
FROM system.numbers LIMIT 100;

-- bad case - rapidly growing values
INSERT INTO codecArrayTest
SELECT 
    number + 200 AS key,
    'i*sqrt(i)' AS name,
    arrayMap(i -> i * sqrt(i), range(100)) AS v64,
    arrayMap(i -> toFloat32(i * sqrt(i)), range(100)) AS v32,
    v64, v32
FROM system.numbers LIMIT 100;

-- worst case - pseudo-random
INSERT INTO codecArrayTest
SELECT 
    number + 300 AS key,
    'sin(i*i*i)*i' AS name,
    arrayMap(i -> sin(i * i * i) * i, range(100)) AS v64,
    arrayMap(i -> toFloat32(sin(i * i * i) * i), range(100)) AS v32,
    v64, v32
FROM system.numbers LIMIT 100;

-- Comparing F64: sum of absolute errors / sum of original
SELECT 'F64: summed relative error';
SELECT 
    name,
    sum(arrayReduce('max', arrayMap((r, v) -> abs(r - v), ref_valueF64, valueF64))) AS abs_error,
    sum(arrayReduce('max', arrayMap(r -> abs(r), ref_valueF64))) AS abs_total,
    abs_error / abs_total AS rel_error
FROM codecArrayTest
GROUP BY name
HAVING rel_error > 0.02;

-- Comparing F32: sum of absolute errors / sum of original
SELECT 'F32: summed relative error';
SELECT 
    name,
    sum(arrayReduce('max', arrayMap((r, v) -> abs(r - v), ref_valueF32, valueF32))) AS abs_error,
    sum(arrayReduce('max', arrayMap(r -> abs(r), ref_valueF32))) AS abs_total,
    abs_error / abs_total AS rel_error
FROM codecArrayTest
GROUP BY name
HAVING rel_error > 0.02;

DROP TABLE codecArrayTest;
