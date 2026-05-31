-- Tags: no-random-merge-tree-settings

-- Test for ByteStreamSplit codec — type compatibility, explicit width parameter,
-- codec chaining, unaligned input sizes, and invalid parameter rejection.

SET allow_experimental_codecs = 1;

DROP TABLE IF EXISTS tab;


-- ── Valid type combinations ────────────────────────────────────────────────────

-- Explicit width parameter: ByteStreamSplit(4) is Float32-width
CREATE TABLE tab (v Float32 CODEC(ByteStreamSplit(4), LZ4)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO tab VALUES (1.0), (2.5), (3.14);
SELECT count() FROM tab;
DROP TABLE tab;

-- Explicit width 8 (Float64/Int64/UInt64)
CREATE TABLE tab (v Float64 CODEC(ByteStreamSplit(8), LZ4)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO tab VALUES (1.0), (1e100), (-1e-100);
SELECT count() FROM tab;
DROP TABLE tab;

-- Explicit width 16 (Int128/UUID/IPv6/FixedString(16))
CREATE TABLE tab (v FixedString(16) CODEC(ByteStreamSplit(16), ZSTD)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO tab SELECT reinterpret(generateUUIDv4(), 'FixedString(16)') FROM numbers(10);
SELECT count() FROM tab;
DROP TABLE tab;

-- Width inferred from UInt16 → W=2
CREATE TABLE tab (v UInt16 CODEC(ByteStreamSplit, LZ4)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO tab SELECT number FROM numbers(500);
SELECT count() FROM tab;
DROP TABLE tab;

-- Width inferred from Int32 → W=4
CREATE TABLE tab (v Int32 CODEC(ByteStreamSplit, LZ4)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO tab SELECT number FROM numbers(500);
SELECT count() FROM tab;
DROP TABLE tab;

-- Width inferred from UInt64 → W=8
CREATE TABLE tab (v UInt64 CODEC(ByteStreamSplit, LZ4)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO tab SELECT number FROM numbers(500);
SELECT count() FROM tab;
DROP TABLE tab;

-- FixedString with explicit width matching size
CREATE TABLE tab (v FixedString(4) CODEC(ByteStreamSplit(4), LZ4)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO tab VALUES ('ABCD'), ('1234'), ('....');
SELECT count() FROM tab;
DROP TABLE tab;


-- ── Chaining with compressors ──────────────────────────────────────────────────

-- ByteStreamSplit + LZ4
CREATE TABLE tab (v Float64 CODEC(ByteStreamSplit, LZ4)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO tab SELECT sin(number) FROM numbers(1000);
SELECT count() FROM tab;
DROP TABLE tab;

-- ByteStreamSplit + ZSTD(1)
CREATE TABLE tab (v Float64 CODEC(ByteStreamSplit, ZSTD(1))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO tab SELECT sin(number) FROM numbers(1000);
SELECT count() FROM tab;
DROP TABLE tab;

-- ByteStreamSplit + LZ4HC
CREATE TABLE tab (v Float32 CODEC(ByteStreamSplit, LZ4HC(9))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO tab SELECT toFloat32(sin(number)) FROM numbers(1000);
SELECT count() FROM tab;
DROP TABLE tab;


-- ── Runtime-width and W=16 roundtrip over non-standard FixedString widths ──────
-- These widths (3 and 5) go down the runtime-W path (encodeRuntime/decodeRuntime)
-- rather than the compile-time encodeW<W> specialisations.
--
CREATE TABLE tab (v FixedString(3) CODEC(ByteStreamSplit(3), LZ4)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO tab SELECT toFixedString(leftPad(toString(number), 3, '0'), 3) FROM numbers(101);
-- Force a full read of `v` so the codec's decode path actually runs, then verify each byte roundtripped.
SELECT 'FS3 roundtrip rows / mismatches:', count(), countIf(v != toFixedString(leftPad(toString(toUInt32(v)), 3, '0'), 3)) FROM tab;
DROP TABLE tab;

CREATE TABLE tab (v FixedString(5) CODEC(ByteStreamSplit(5), LZ4)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO tab SELECT toFixedString(leftPad(toString(number), 5, '0'), 5) FROM numbers(7);
SELECT 'FS5 roundtrip rows / mismatches:', count(), countIf(v != toFixedString(leftPad(toString(toUInt64(v)), 5, '0'), 5)) FROM tab;
SELECT v FROM tab ORDER BY v;
DROP TABLE tab;


-- ── Correctness spot-check with known Float32 values ──────────────────────────
CREATE TABLE tab
(
    ref  Float32,
    bss  Float32 CODEC(ByteStreamSplit, LZ4)
) ENGINE = MergeTree ORDER BY ref;

INSERT INTO tab (ref, bss) VALUES
    (0.0, 0.0),
    (1.0, 1.0),
    (-1.0, -1.0),
    (3.14159274, 3.14159274),
    (1e38, 1e38),
    (-1e-38, -1e-38),
    (toFloat32('inf'), toFloat32('inf')),
    (toFloat32('-inf'), toFloat32('-inf')),
    (toFloat32('nan'), toFloat32('nan'));

-- No diffs expected between ref and bss
SELECT 'F32 spot-check diffs:';
SELECT ref, bss FROM tab WHERE NOT isNaN(ref) AND ref != bss LIMIT 10;
-- NaN is never equal to itself; verify byte identity via reinterpret
SELECT 'F32 NaN identity:';
SELECT reinterpretAsUInt32(ref) = reinterpretAsUInt32(bss) AS same_bits FROM tab WHERE isNaN(ref);

DROP TABLE tab;


-- ── Correctness spot-check with known Float64 values ──────────────────────────
CREATE TABLE tab
(
    ref  Float64,
    bss  Float64 CODEC(ByteStreamSplit, LZ4)
) ENGINE = MergeTree ORDER BY ref;

INSERT INTO tab (ref, bss) VALUES
    (0.0, 0.0),
    (1.0, 1.0),
    (-1.0, -1.0),
    (2.718281828459045, 2.718281828459045),
    (1e308, 1e308),
    (-1e-308, -1e-308),
    (toFloat64('inf'), toFloat64('inf')),
    (toFloat64('-inf'), toFloat64('-inf'));

SELECT 'F64 spot-check diffs:';
SELECT ref, bss FROM tab WHERE ref != bss LIMIT 10;

DROP TABLE tab;


-- ── Invalid parameter rejection ────────────────────────────────────────────────

-- UInt8 has element size 1, which is below the minimum of 2
CREATE TABLE tab (v UInt8 CODEC(ByteStreamSplit)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

-- Explicit width 1 is too small
CREATE TABLE tab (v UInt16 CODEC(ByteStreamSplit(1))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError ILLEGAL_CODEC_PARAMETER }

-- Explicit width 0 is invalid
CREATE TABLE tab (v UInt16 CODEC(ByteStreamSplit(0))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError ILLEGAL_CODEC_PARAMETER }

-- Explicit width 256 exceeds MAX_ELEMENT_WIDTH (255)
CREATE TABLE tab (v FixedString(256) CODEC(ByteStreamSplit(256))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError ILLEGAL_CODEC_PARAMETER }

-- String is not fixed-size, must be rejected
CREATE TABLE tab (v String CODEC(ByteStreamSplit)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

-- LowCardinality(String) is not fixed-size, must be rejected
CREATE TABLE tab (v LowCardinality(String) CODEC(ByteStreamSplit)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

-- Too many codec arguments (accepts at most 1)
CREATE TABLE tab (v Float32 CODEC(ByteStreamSplit(4, 4))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError ILLEGAL_SYNTAX_FOR_CODEC_TYPE }


-- ── Array(fixed-size) round-trip ──────────────────────────────────────────────
-- ByteStreamSplit is applied per substream: the inner Float32 element stream
-- is transposed (W=4) and the offsets substream goes through the chained
-- compressor. Arrays of fixed-size elements are supported.

CREATE TABLE tab (v Array(Float32) CODEC(ByteStreamSplit, LZ4)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO tab VALUES ([1.0, 2.0, 3.0]), ([4.0, 5.0]), ([6.0]), ([]);
SELECT 'Array(Float32) round-trip:';
SELECT v FROM tab ORDER BY length(v), v;
DROP TABLE tab;
