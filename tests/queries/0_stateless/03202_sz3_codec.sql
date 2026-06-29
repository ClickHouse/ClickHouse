-- Tags: no-fasttest
-- no-fasttest: needs sz3 library

SET allow_experimental_codecs = 1;
SET cross_to_inner_join_rewrite = 1;
SET async_insert = 1;

DROP TABLE IF EXISTS tab;

SELECT 'Negative tests';

-- SZ3 can only be created on Float* and Array(Float*) columns
CREATE TABLE tab (compressed String CODEC(SZ3)) Engine = Memory; -- { serverError BAD_ARGUMENTS }
CREATE TABLE tab (compressed UInt64 CODEC(SZ3)) Engine = Memory; -- { serverError BAD_ARGUMENTS }
CREATE TABLE tab (compressed BFloat16 CODEC(SZ3)) Engine = Memory; -- { serverError BAD_ARGUMENTS }
CREATE TABLE tab (compressed Array(UInt64) CODEC(SZ3)) Engine = Memory; -- { serverError BAD_ARGUMENTS }
CREATE TABLE tab_map (
    compressed_f64   Map(UInt8, Float64) CODEC(SZ3('ALGO_INTERP', 'REL', 0.01)),
    compressed_f32   Map(UInt8, Float32) CODEC(SZ3('ALGO_INTERP', 'REL', 0.01))
) ENGINE = Memory; -- { serverError BAD_ARGUMENTS }

-- SZ3 for array columns requires that all inserted arrays have the same cardinality
CREATE TABLE tab (key UInt64, val Array(Float64) CODEC(SZ3)) ENGINE = MergeTree ORDER BY key;
INSERT INTO tab VALUES (1, [1.0, 2.0]) (2, [3.0, 4.0, 5.0]); -- { serverError BAD_ARGUMENTS }

DROP TABLE IF EXISTS tab;

-- SZ3 requires 0 or 3 arguments
CREATE TABLE tab (compressed Float64 CODEC(SZ3('ALGO_INTERP'))) Engine = Memory; -- { serverError BAD_ARGUMENTS }
CREATE TABLE tab (compressed Float64 CODEC(SZ3('ALGO_INTERP', 'REL', 0.01, 1))) Engine = Memory; -- { serverError BAD_ARGUMENTS }

-- The 1st argument must be a string
CREATE TABLE tab (compressed Float64 CODEC(SZ3(1, 'REL', 0.01))) Engine = Memory; -- { serverError ILLEGAL_CODEC_PARAMETER }
-- The 2nd argument must be a string
CREATE TABLE tab (compressed Float64 CODEC(SZ3('ALGO_INTERP', 1, 0.01))) Engine = Memory; -- { serverError ILLEGAL_CODEC_PARAMETER }
-- The 3rd argument must be a Float64
CREATE TABLE tab (compressed Float64 CODEC(SZ3('ALGO_INTERP', 'REL', 'not_a_f64'))) Engine = Memory; -- { serverError ILLEGAL_CODEC_PARAMETER }

SELECT 'Test wide/compact format';
-- Very basic test to make sure nothing breaks

DROP TABLE IF EXISTS tab_wide;
DROP TABLE IF EXISTS tab_compact;

CREATE TABLE tab_wide(id Int32, vec Array(Float32) CODEC(SZ3)) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
CREATE TABLE tab_compact(id Int32, vec Array(Float32) CODEC(SZ3)) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 1e9, min_rows_for_wide_part = 1e9;

INSERT INTO tab_wide VALUES (0, [1.0, 0.0]), (1, [1.1, 0.0]), (2, [1.2, 0.0]), (3, [1.3, 0.0]), (4, [1.4, 0.0]), (5, [1.5, 0.0]), (6, [0.0, 2.0]), (7, [0.0, 2.1]), (8, [0.0, 2.2]), (9, [0.0, 2.3]), (10, [0.0, 2.4]), (11, [0.0, 2.5]);
INSERT INTO tab_compact VALUES (0, [1.0, 0.0]), (1, [1.1, 0.0]), (2, [1.2, 0.0]), (3, [1.3, 0.0]), (4, [1.4, 0.0]), (5, [1.5, 0.0]), (6, [0.0, 2.0]), (7, [0.0, 2.1]), (8, [0.0, 2.2]), (9, [0.0, 2.3]), (10, [0.0, 2.4]), (11, [0.0, 2.5]);

SELECT * FROM tab_wide ORDER BY ALL;
SELECT * FROM tab_compact ORDER BY ALL;

DROP TABLE tab_wide;

SELECT 'Test with default settings';

CREATE TABLE tab (
    key                 UInt64,
    name                String,
    uncompressed_f64    Float64,
    uncompressed_f32    Float32,
    compressed_f64      Float64  CODEC(SZ3),
    compressed_f32      Float32  CODEC(SZ3)
) Engine = MergeTree ORDER BY key;

-- best case - same value
INSERT INTO tab (key, name, uncompressed_f64, uncompressed_f32, compressed_f64, compressed_f32)
    SELECT number AS n, 'e()', e() AS v, v, v, v FROM system.numbers LIMIT 1, 100;

-- good case - values that grow insignificantly
INSERT INTO tab (key, name, uncompressed_f64, uncompressed_f32, compressed_f64, compressed_f32)
    SELECT number AS n, 'log2(n)', log2(n) AS v, v, v, v FROM system.numbers LIMIT 101, 100;

-- bad case - values differ significantly
INSERT INTO tab (key, name, uncompressed_f64, uncompressed_f32, compressed_f64, compressed_f32)
    SELECT number AS n, 'n*sqrt(n)', n * sqrt(n) AS v, v, v, v FROM system.numbers LIMIT 201, 100;

-- worst case - almost like a random values
INSERT INTO tab (key, name, uncompressed_f64, uncompressed_f32, compressed_f64, compressed_f32)
    SELECT number AS n, 'sin(n*n*n)*n', sin(n * n * n)*n AS v, v, v, v FROM system.numbers LIMIT 301, 100;

SELECT '-- Test F64';
SELECT
    c1.key, c1.name,
    c1.uncompressed_f64, c1.compressed_f64, c1.uncompressed_f64 - c1.compressed_f64 AS diff_f64,
    'prev:',
    c2.key, c2.uncompressed_f64
FROM
    tab as c1, tab as c2
WHERE
    abs(1 - diff_f64 / c1.uncompressed_f64) < 0.01
AND
    c2.key = c1.key - 1
LIMIT 10;

SELECT '-- Test F32';
SELECT
    c1.key, c1.name,
    c1.uncompressed_f32, c1.compressed_f32, c1.uncompressed_f32 - c1.compressed_f32 AS diff_f32,
    'prev:',
    c2.key, c2.uncompressed_f32
FROM
    tab as c1, tab as c2
WHERE
    abs(1 - diff_f32 / c1.uncompressed_f32) < 0.01
AND
    c2.key = c1.key - 1
LIMIT 10;

DROP TABLE tab;

SELECT 'Test with custom settings';

CREATE TABLE tab (
    key                 UInt64,
    name                String,
    uncompressed_f64    Float64,
    uncompressed_f32    Float32,
    compressed_f64      Float64  CODEC(SZ3('ALGO_INTERP', 'REL', 0.01)),
    compressed_f32      Float32  CODEC(SZ3('ALGO_INTERP', 'REL', 0.01))
) Engine = MergeTree ORDER BY key;

-- best case - same value
INSERT INTO tab (key, name, uncompressed_f64, uncompressed_f32, compressed_f64, compressed_f32)
    SELECT number AS n, 'e()', e() AS v, v, v, v FROM system.numbers LIMIT 1, 100;

-- good case - values that grow insignificantly
INSERT INTO tab (key, name, uncompressed_f64, uncompressed_f32, compressed_f64, compressed_f32)
    SELECT number AS n, 'log2(n)', log2(n) AS v, v, v, v FROM system.numbers LIMIT 101, 100;

-- bad case - values differ significantly
INSERT INTO tab (key, name, uncompressed_f64, uncompressed_f32, compressed_f64, compressed_f32)
    SELECT number AS n, 'n*sqrt(n)', n*sqrt(n) AS v, v, v, v FROM system.numbers LIMIT 201, 100;

-- worst case - almost like a random values
INSERT INTO tab (key, name, uncompressed_f64, uncompressed_f32, compressed_f64, compressed_f32)
    SELECT number AS n, 'sin(n*n*n)*n', sin(n * n * n)*n AS v, v, v, v FROM system.numbers LIMIT 301, 100;

SELECT '-- Test F64';
SELECT
    c1.key, c1.name,
    c1.uncompressed_f64, c1.compressed_f64, c1.uncompressed_f64 - c1.compressed_f64 AS diff_f64,
    'prev:',
    c2.key, c2.uncompressed_f64
FROM
    tab as c1, tab as c2
WHERE
    abs(1 - diff_f64 / c1.uncompressed_f64) < 0.01
AND
    c2.key = c1.key - 1
LIMIT 10;


SELECT '-- Test F32';
SELECT
    c1.key, c1.name,
    c1.uncompressed_f32, c1.compressed_f32, c1.uncompressed_f32 - c1.compressed_f32 AS diff_f32,
    'prev:',
    c2.key, c2.uncompressed_f32
FROM
    tab as c1, tab as c2
WHERE
    abs(1 - diff_f32 / c1.uncompressed_f32) < 0.01
AND
    c2.key = c1.key - 1
LIMIT 10;

SELECT 'Test 1d array';

DROP TABLE IF EXISTS tab;

CREATE TABLE tab (
    key              UInt64,
    name             String,
    uncompressed_f64 Array(Float64),
    uncompressed_f32 Array(Float32),
    compressed_f64   Array(Float64) CODEC(SZ3('ALGO_INTERP', 'REL', 0.01)),
    compressed_f32   Array(Float32) CODEC(SZ3('ALGO_INTERP', 'REL', 0.01))
) ENGINE = MergeTree ORDER BY key;

-- best case - all elements equal
INSERT INTO tab
SELECT
    number AS key,
    'constant array' AS name,
    arrayResize([e()], 10) AS v64,
    arrayResize([toFloat32(e())], 10) AS v32,
    v64, v32
FROM system.numbers LIMIT 10;

-- good case - slightly growing values
INSERT INTO tab
SELECT
    number + 100 AS key,
    'log(1..10)' AS name,
    arrayMap(i -> log(number + i + 1), range(100)) AS v64,
    arrayMap(i -> toFloat32(log(number + i + 1)), range(100)) AS v32,
    v64, v32
FROM system.numbers LIMIT 100;

-- bad case - rapidly growing values
INSERT INTO tab
SELECT
    number + 200 AS key,
    'i*sqrt(i)' AS name,
    arrayMap(i -> i * sqrt(i), range(100)) AS v64,
    arrayMap(i -> toFloat32(i * sqrt(i)), range(100)) AS v32,
    v64, v32
FROM system.numbers LIMIT 100;

-- worst case - pseudo-random
INSERT INTO tab
SELECT
    number + 300 AS key,
    'sin(i*i*i)*i' AS name,
    arrayMap(i -> sin(i * i * i) * i, range(100)) AS v64,
    arrayMap(i -> toFloat32(sin(i * i * i) * i), range(100)) AS v32,
    v64, v32
FROM system.numbers LIMIT 100;

-- Comparing F64: sum of absolute errors / sum of original
SELECT '-- Test F64: summed relative error';
SELECT
    name,
    sum(arrayReduce('max', arrayMap((r, v) -> abs(r - v), uncompressed_f64, compressed_f64))) AS abs_error,
    sum(arrayReduce('max', arrayMap(r -> abs(r), uncompressed_f64))) AS abs_total,
    abs_error / abs_total AS rel_error
FROM tab
GROUP BY name
HAVING rel_error > 0.02;

-- Comparing F32: sum of absolute errors / sum of original
SELECT '-- Test F32: summed relative error';
SELECT
    name,
    sum(arrayReduce('max', arrayMap((r, v) -> abs(r - v), uncompressed_f32, compressed_f32))) AS abs_error,
    sum(arrayReduce('max', arrayMap(r -> abs(r), uncompressed_f32))) AS abs_total,
    abs_error / abs_total AS rel_error
FROM tab
GROUP BY name
HAVING rel_error > 0.02;

DROP TABLE tab;

SELECT 'Simple test 1d tuple';

DROP TABLE IF EXISTS tab;

CREATE TABLE tab (
    key              UInt64,
    name             String,
    uncompressed_f64 Tuple(Float64),
    uncompressed_f32 Tuple(Float32),
    compressed_f64   Tuple(Float64) CODEC(SZ3('ALGO_INTERP', 'REL', 0.01)),
    compressed_f32   Tuple(Float32) CODEC(SZ3('ALGO_INTERP', 'REL', 0.01))
) ENGINE = MergeTree ORDER BY key;

INSERT INTO tab VALUES
(
    1,
    'alpha',
    (123.456),
    (12.34),
    (123.456),
    (12.34)
),
(
    2,
    'beta',
    (654.321),
    (43.21),
    (654.321),
    (43.21)
),
(
    3,
    'gamma',
    (111.222),
    (22.11),
    (111.222),
    (22.11)
),
(
    4,
    'delta',
    (0.001),
    (0.002),
    (0.001),
    (0.002)
),
(
    5,
    'epsilon',
    (9999.999),
    (999.99),
    (9999.999),
    (999.99)
);

SELECT
    c1.key,
    c1.name,
    c1.uncompressed_f64,
    c1.compressed_f64,
    tupleElement(c1.uncompressed_f64, 1) - tupleElement(c1.compressed_f64, 1) AS diff_f64,
    'prev:' AS marker,
    c2.key AS prev_key,
    c2.uncompressed_f64 AS prev_uncompressed_f64
FROM
    tab AS c1
INNER JOIN
    tab AS c2
    ON c2.key = toUInt64(c1.key - 1)
WHERE
    abs(1 - (tupleElement(c1.uncompressed_f64, 1) - tupleElement(c1.compressed_f64, 1)) / tupleElement(c1.uncompressed_f64, 1)) < 0.01
LIMIT 10;

DROP TABLE tab;
