-- Tags: no-random-merge-tree-settings

-- Test for ByteStreamSplit codec correctness and compression.
-- ByteStreamSplit is a preprocessing codec that transposes bytes of fixed-width
-- elements (W bytes each → W interleaved streams of N bytes).
-- It must exactly preserve every bit of every value.

DROP TABLE IF EXISTS codecTest;

CREATE TABLE codecTest
(
    key           UInt64,
    name          String,

    -- reference columns (default LZ4 via MergeTree)
    ref_valueF64  Float64,
    ref_valueF32  Float32,
    ref_valueI64  Int64,
    ref_valueI32  Int32,
    ref_valueI16  Int16,
    ref_valueU64  UInt64,
    ref_valueU32  UInt32,
    ref_valueU16  UInt16,

    -- ByteStreamSplit columns (codec inferred from data type)
    valueF64  Float64   CODEC(ByteStreamSplit, LZ4),
    valueF32  Float32   CODEC(ByteStreamSplit, LZ4),
    valueI64  Int64     CODEC(ByteStreamSplit, LZ4),
    valueI32  Int32     CODEC(ByteStreamSplit, LZ4),
    valueI16  Int16     CODEC(ByteStreamSplit, LZ4),
    valueU64  UInt64    CODEC(ByteStreamSplit, LZ4),
    valueU32  UInt32    CODEC(ByteStreamSplit, LZ4),
    valueU16  UInt16    CODEC(ByteStreamSplit, LZ4)

) ENGINE = MergeTree ORDER BY key
  SETTINGS min_bytes_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1;


-- best case: constant value (all bytes in every stream are identical)
INSERT INTO codecTest (key, name,
    ref_valueF64, valueF64, ref_valueF32, valueF32,
    ref_valueI64, valueI64, ref_valueI32, valueI32, ref_valueI16, valueI16,
    ref_valueU64, valueU64, ref_valueU32, valueU32, ref_valueU16, valueU16)
    SELECT number AS n, 'constant(e())',
        e(), e(), toFloat32(e()), toFloat32(e()),
        1000, 1000, 1000, 1000, 1000, 1000,
        1000, 1000, 1000, 1000, 1000, 1000
    FROM system.numbers LIMIT 1, 1000;

-- good case: slowly growing floats (exponent bytes stay similar → BSS shines)
INSERT INTO codecTest (key, name,
    ref_valueF64, valueF64, ref_valueF32, valueF32,
    ref_valueI64, valueI64, ref_valueI32, valueI32, ref_valueI16, valueI16,
    ref_valueU64, valueU64, ref_valueU32, valueU32, ref_valueU16, valueU16)
    SELECT number AS n, 'log2(n)',
        log2(n) AS v, v, toFloat32(v), toFloat32(v),
        toInt64(n), toInt64(n), toInt32(n), toInt32(n), toInt16(n % 32767), toInt16(n % 32767),
        n, n, toUInt32(n), toUInt32(n), toUInt16(n % 65535), toUInt16(n % 65535)
    FROM system.numbers LIMIT 1001, 1000;

-- bad case: n^3 → fast-growing integers and large float magnitude swings
INSERT INTO codecTest (key, name,
    ref_valueF64, valueF64, ref_valueF32, valueF32,
    ref_valueI64, valueI64, ref_valueI32, valueI32, ref_valueI16, valueI16,
    ref_valueU64, valueU64, ref_valueU32, valueU32, ref_valueU16, valueU16)
    SELECT number AS n, 'n*n*n',
        toFloat64(n * n * n) AS v, v, toFloat32(v), toFloat32(v),
        toInt64(n * n * n), toInt64(n * n * n),
        toInt32(n * n * n), toInt32(n * n * n),
        toInt16((n * n) % 32767), toInt16((n * n) % 32767),
        toUInt64(n * n * n), toUInt64(n * n * n),
        toUInt32(n * n * n), toUInt32(n * n * n),
        toUInt16((n * n) % 65535), toUInt16((n * n) % 65535)
    FROM system.numbers LIMIT 2001, 1000;

-- worst case: pseudorandom (sin(n^5)) — hardest for any codec
INSERT INTO codecTest (key, name,
    ref_valueF64, valueF64, ref_valueF32, valueF32,
    ref_valueI64, valueI64, ref_valueI32, valueI32, ref_valueI16, valueI16,
    ref_valueU64, valueU64, ref_valueU32, valueU32, ref_valueU16, valueU16)
    SELECT number AS n, 'sin(n^5)*n',
        sin(n * n * n * n * n) * n AS v, v, toFloat32(v), toFloat32(v),
        toInt64(v * 1000), toInt64(v * 1000),
        toInt32(v * 1000), toInt32(v * 1000),
        toInt16(toInt64(v * 100) % 32767), toInt16(toInt64(v * 100) % 32767),
        toUInt64(abs(toInt64(v * 1000))), toUInt64(abs(toInt64(v * 1000))),
        toUInt32(abs(toInt32(v * 1000))), toUInt32(abs(toInt32(v * 1000))),
        toUInt16(abs(toInt16(toInt64(v * 100) % 32767))), toUInt16(abs(toInt16(toInt64(v * 100) % 32767)))
    FROM system.numbers LIMIT 3001, 1000;


-- Verify lossless roundtrip: any mismatch between ref and codec column is a bug.
-- Because ByteStreamSplit is a lossless byte permutation, no diffs are expected.

SELECT 'F64';
SELECT key, name, ref_valueF64, valueF64, ref_valueF64 - valueF64 AS dF64
FROM codecTest WHERE dF64 != 0 LIMIT 10;

SELECT 'F32';
SELECT key, name, ref_valueF32, valueF32, ref_valueF32 - valueF32 AS dF32
FROM codecTest WHERE dF32 != 0 LIMIT 10;

SELECT 'I64';
SELECT key, name, ref_valueI64, valueI64, ref_valueI64 - valueI64 AS dI64
FROM codecTest WHERE dI64 != 0 LIMIT 10;

SELECT 'I32';
SELECT key, name, ref_valueI32, valueI32, ref_valueI32 - valueI32 AS dI32
FROM codecTest WHERE dI32 != 0 LIMIT 10;

SELECT 'I16';
SELECT key, name, ref_valueI16, valueI16, ref_valueI16 - valueI16 AS dI16
FROM codecTest WHERE dI16 != 0 LIMIT 10;

SELECT 'U64';
SELECT key, name, ref_valueU64, valueU64, ref_valueU64 - valueU64 AS dU64
FROM codecTest WHERE dU64 != 0 LIMIT 10;

SELECT 'U32';
SELECT key, name, ref_valueU32, valueU32, ref_valueU32 - valueU32 AS dU32
FROM codecTest WHERE dU32 != 0 LIMIT 10;

SELECT 'U16';
SELECT key, name, ref_valueU16, valueU16, ref_valueU16 - valueU16 AS dU16
FROM codecTest WHERE dU16 != 0 LIMIT 10;

-- Verify that ByteStreamSplit columns have a codec stored in system.columns
SELECT 'Compression:';
SELECT name, type, compression_codec
FROM system.columns
WHERE table = 'codecTest'
  AND database = currentDatabase()
  AND compression_codec LIKE '%ByteStreamSplit%'
ORDER BY name;

DROP TABLE IF EXISTS codecTest;
