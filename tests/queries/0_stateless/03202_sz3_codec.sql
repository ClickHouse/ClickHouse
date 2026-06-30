-- Tags: no-fasttest, no-random-settings, no-random-merge-tree-settings
-- no-fasttest: needs sz3 library
-- no-random-settings, no-random-merge-tree-settings: SZ3 is lossy and its output depends on how the data
-- is split into compressed blocks (part type, block sizes), so the error-magnitude assertions below are
-- only stable with fixed settings.

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

-- SZ3 is lossy and would round Map keys on disk, so Map columns are rejected even when the keys are floats
CREATE TABLE tab_map (
    compressed_f64   Map(Float64, Float64) CODEC(SZ3('ALGO_INTERP', 'REL', 0.01)),
    compressed_f32   Map(Float32, Float32) CODEC(SZ3('ALGO_INTERP', 'REL', 0.01))
) ENGINE = Memory; -- { serverError BAD_ARGUMENTS }

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

-- Only ALGO_LORENZO_REG, ALGO_INTERP_LORENZO and ALGO_INTERP are supported
CREATE TABLE tab (compressed Float64 CODEC(SZ3('ALGO_BIOMD', 'REL', 0.01))) Engine = Memory; -- { serverError ILLEGAL_CODEC_PARAMETER }
CREATE TABLE tab (compressed Float64 CODEC(SZ3('ALGO_LOSSLESS', 'REL', 0.01))) Engine = Memory; -- { serverError ILLEGAL_CODEC_PARAMETER }

-- Only ABS, REL, PSNR and ABS_AND_REL error bound modes are supported
CREATE TABLE tab (compressed Float64 CODEC(SZ3('ALGO_INTERP', 'NOT_A_MODE', 0.01))) Engine = Memory; -- { serverError ILLEGAL_CODEC_PARAMETER }

-- The error bound must be a finite positive number (a non-finite or non-positive bound would make the quantizer produce out-of-range indices)
CREATE TABLE tab (compressed Float64 CODEC(SZ3('ALGO_INTERP', 'ABS', 0))) Engine = Memory; -- { serverError ILLEGAL_CODEC_PARAMETER }
CREATE TABLE tab (compressed Float64 CODEC(SZ3('ALGO_INTERP', 'ABS', -1))) Engine = Memory; -- { serverError ILLEGAL_CODEC_PARAMETER }
CREATE TABLE tab (compressed Float64 CODEC(SZ3('ALGO_INTERP', 'ABS', nan))) Engine = Memory; -- { serverError ILLEGAL_CODEC_PARAMETER }
CREATE TABLE tab (compressed Float64 CODEC(SZ3('ALGO_INTERP', 'ABS', inf))) Engine = Memory; -- { serverError ILLEGAL_CODEC_PARAMETER }

-- SZ3 must be applied to raw float data, so it can not follow another codec
CREATE TABLE tab (compressed Float64 CODEC(Delta, SZ3)) Engine = Memory; -- { serverError BAD_ARGUMENTS }

SELECT 'Array columns with different lengths across parts';
-- Two separate inserts create two parts whose arrays have different cardinalities. Each part is internally
-- consistent, but a forced merge combines both through a single SZ3 codec instance. SZ3 must not get stuck
-- on the differing dimensions (which would leave background merges permanently failing on data that
-- individual inserts accepted): it falls back to flat 1D compression for the merged part. The array
-- structure is preserved exactly; only the float values are lossy (here within the ABS error bound).

DROP TABLE IF EXISTS tab_merge_wide;
DROP TABLE IF EXISTS tab_merge_compact;

CREATE TABLE tab_merge_wide (key UInt64, orig Array(Float64), val Array(Float64) CODEC(SZ3('ALGO_INTERP', 'ABS', 0.01)))
    ENGINE = MergeTree ORDER BY key SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
CREATE TABLE tab_merge_compact (key UInt64, orig Array(Float64), val Array(Float64) CODEC(SZ3('ALGO_INTERP', 'ABS', 0.01)))
    ENGINE = MergeTree ORDER BY key SETTINGS min_bytes_for_wide_part = 1e9, min_rows_for_wide_part = 1e9;

SYSTEM STOP MERGES tab_merge_wide;
SYSTEM STOP MERGES tab_merge_compact;

INSERT INTO tab_merge_wide VALUES (1, [10.0, 20.0], [10.0, 20.0]);
INSERT INTO tab_merge_wide VALUES (2, [30.0, 40.0, 50.0], [30.0, 40.0, 50.0]);
INSERT INTO tab_merge_compact VALUES (1, [10.0, 20.0], [10.0, 20.0]);
INSERT INTO tab_merge_compact VALUES (2, [30.0, 40.0, 50.0], [30.0, 40.0, 50.0]);

SELECT '-- before merge: two parts each';
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'tab_merge_wide' AND active;
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'tab_merge_compact' AND active;

SYSTEM START MERGES tab_merge_wide;
SYSTEM START MERGES tab_merge_compact;
OPTIMIZE TABLE tab_merge_wide FINAL;
OPTIMIZE TABLE tab_merge_compact FINAL;

SELECT '-- after merge: one part each, lengths preserved, values within the error bound';
SELECT key, length(val), arrayMax(arrayMap((o, r) -> abs(o - r), orig, val)) <= 0.05 AS within_error
FROM tab_merge_wide ORDER BY key;
SELECT key, length(val), arrayMax(arrayMap((o, r) -> abs(o - r), orig, val)) <= 0.05 AS within_error
FROM tab_merge_compact ORDER BY key;
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'tab_merge_wide' AND active;
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'tab_merge_compact' AND active;

DROP TABLE tab_merge_wide;
DROP TABLE tab_merge_compact;

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

SELECT 'SZ3 is lossy and cannot be used where the column data type is unknown';
-- The marks and the primary key are not floating-point data, and the codec is built for them without a data
-- type, so a lossy codec there would silently corrupt them. The setting is accepted at CREATE but rejected
-- when the part is written.
DROP TABLE IF EXISTS tab_marks_codec;
CREATE TABLE tab_marks_codec (x Float64 CODEC(SZ3)) ENGINE = MergeTree ORDER BY tuple() SETTINGS marks_compression_codec = 'SZ3';
INSERT INTO tab_marks_codec VALUES (1.5); -- { serverError BAD_ARGUMENTS }
DROP TABLE tab_marks_codec;

DROP TABLE IF EXISTS tab_pk_codec;
CREATE TABLE tab_pk_codec (x Float64 CODEC(SZ3)) ENGINE = MergeTree ORDER BY x SETTINGS primary_key_compression_codec = 'SZ3';
INSERT INTO tab_pk_codec VALUES (1.5); -- { serverError BAD_ARGUMENTS }
DROP TABLE tab_pk_codec;

SELECT 'A compression block size that is not a multiple of the float width is rejected';
-- SZ3 compresses whole values. `CompressedWriteBuffer` chunks the column stream into compressed blocks by the
-- `max_compress_block_size` byte count, so a value is split across two blocks when that setting is not a
-- multiple of the value width. Such a block previously compressed only the whole-value prefix and silently
-- dropped the trailing bytes, while the block header still recorded the full uncompressed size; the part was
-- accepted on insert but could not be read back. It is now rejected at write time with a clear error.
DROP TABLE IF EXISTS tab_block_size_f64;
CREATE TABLE tab_block_size_f64 (x Float64 CODEC(SZ3)) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS max_compress_block_size = 10, min_bytes_for_wide_part = 0;
INSERT INTO tab_block_size_f64 SELECT number FROM numbers(100) SETTINGS async_insert = 0; -- { serverError BAD_ARGUMENTS }
DROP TABLE tab_block_size_f64;

DROP TABLE IF EXISTS tab_block_size_f32;
CREATE TABLE tab_block_size_f32 (x Float32 CODEC(SZ3)) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS max_compress_block_size = 5, min_bytes_for_wide_part = 0;
INSERT INTO tab_block_size_f32 SELECT number FROM numbers(100) SETTINGS async_insert = 0; -- { serverError BAD_ARGUMENTS }
DROP TABLE tab_block_size_f32;

SELECT 'The lossless (zstd) fallback round-trips';
-- For data that does not compress well, SZ3 transparently falls back to a plain lossless (zstd) block. The
-- decompressor now validates that this lossless payload declares an output size equal to the trusted
-- uncompressed size before handing it to zstd (a crafted block could otherwise drive an out-of-bounds write),
-- so the legitimate lossless path must still round-trip exactly.
DROP TABLE IF EXISTS tab_lossless;
CREATE TABLE tab_lossless (x Float64 CODEC(SZ3('ALGO_INTERP', 'ABS', 0.0001))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO tab_lossless VALUES (1.5);
SELECT abs(x - 1.5) <= 0.0001 FROM tab_lossless;
DROP TABLE tab_lossless;
