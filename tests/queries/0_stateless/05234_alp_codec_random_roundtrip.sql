-- Bit-exact ALP roundtrip on random data. A failure prints the seed, replace rand64() with it to reproduce.

SET enable_alp_codec = 1;

DROP VIEW IF EXISTS alp_random_check;
DROP TABLE IF EXISTS alp_random;
DROP TABLE IF EXISTS alp_random_seed;

CREATE TABLE alp_random_seed ENGINE = Memory AS SELECT rand64() AS seed;

CREATE TABLE alp_random
(
    i UInt32 CODEC(NONE),
    f64 Float64 CODEC(NONE),
    f64_auto Float64 CODEC(ALP(AUTO)),
    f64_std Float64 CODEC(ALP(STD)),
    f64_rd Float64 CODEC(ALP(RD)),
    f32 Float32 CODEC(NONE),
    f32_auto Float32 CODEC(ALP(AUTO)),
    f32_std Float32 CODEC(ALP(STD)),
    f32_rd Float32 CODEC(ALP(RD))
) ENGINE = MergeTree ORDER BY i;

-- Bits are compared, so NaN payloads and signed zeros count. hex shows them for mismatching rows.
CREATE VIEW alp_random_check AS
SELECT
    (SELECT seed FROM alp_random_seed) AS seed,
    i,
    hex(f64) AS f64_bits,
    hex(f64_auto) AS f64_auto_bits,
    hex(f64_std) AS f64_std_bits,
    hex(f64_rd) AS f64_rd_bits,
    hex(f32) AS f32_bits,
    hex(f32_auto) AS f32_auto_bits,
    hex(f32_std) AS f32_std_bits,
    hex(f32_rd) AS f32_rd_bits,
    reinterpretAsUInt64(f64) != reinterpretAsUInt64(f64_auto)
        OR reinterpretAsUInt64(f64) != reinterpretAsUInt64(f64_std)
        OR reinterpretAsUInt64(f64) != reinterpretAsUInt64(f64_rd)
        OR reinterpretAsUInt32(f32) != reinterpretAsUInt32(f32_auto)
        OR reinterpretAsUInt32(f32) != reinterpretAsUInt32(f32_std)
        OR reinterpretAsUInt32(f32) != reinterpretAsUInt32(f32_rd) AS mismatch
FROM alp_random;

-- The STD path, with a rare full-precision outlier for the exception path.
INSERT INTO alp_random
WITH
    (SELECT seed FROM alp_random_seed) AS seed,
    sipHash64(number, seed) AS h,
    sipHash64(h) AS h2,
    (toInt64(h % 20000000001) - 10000000000) / exp10(h2 % 9) AS dec,
    reinterpretAsFloat64(bitOr(bitAnd(h, 0x000FFFFFFFFFFFFF), 0x3FF0000000000000)) - 1 AS unit,
    if(h2 % 32 = 0, (unit - 0.5) * 1000, dec) AS v64,
    toFloat32(v64) AS v32
SELECT number, v64, v64, v64, v64, v32, v32, v32, v32 FROM numbers(20000);

-- Full-precision values: STD cannot encode them, AUTO must fall back to RD.
INSERT INTO alp_random
WITH
    (SELECT seed FROM alp_random_seed) AS seed,
    sipHash64(number, seed) AS h,
    (reinterpretAsFloat64(bitOr(bitAnd(h, 0x000FFFFFFFFFFFFF), 0x3FF0000000000000)) - 1.5) * 1000 AS v64,
    toFloat32(v64) AS v32
SELECT number + 20000, v64, v64, v64, v64, v32, v32, v32, v32 FROM numbers(20000);

-- Uniformly random bit patterns.
INSERT INTO alp_random
WITH
    (SELECT seed FROM alp_random_seed) AS seed,
    sipHash64(number, seed) AS h,
    reinterpretAsFloat64(h) AS v64,
    reinterpretAsFloat32(toUInt32(h)) AS v32
SELECT number + 40000, v64, v64, v64, v64, v32, v32, v32, v32 FROM numbers(20000);

-- Every other row is a special value. Float32 builds its own, since toFloat32 would alter them.
INSERT INTO alp_random
WITH
    (SELECT seed FROM alp_random_seed) AS seed,
    sipHash64(number, seed) AS h,
    sipHash64(h) AS h2,
    toUInt32(h) AS h32,
    h2 % 16 AS kind,
    if(h % 2 = 0, 1., -1.) AS sgn,
    (toInt64(h % 20000000001) - 10000000000) / exp10(h2 % 9) AS dec,
    multiIf(
        kind = 0, reinterpretAsFloat64(bitOr(bitAnd(h, 0x800FFFFFFFFFFFFF), 0x7FF0000000000001)),
        kind = 1, sgn * inf,
        kind = 2, sgn * 0.,
        kind = 3, reinterpretAsFloat64(bitAnd(h, 0x800FFFFFFFFFFFFF)),
        kind = 4, sgn * 2.2250738585072014e-308,
        kind = 5, sgn * 1.7976931348623157e308,
        kind = 6, sgn * (9223372036854773760. + (toInt64(h % 8192) - 4096)),
        kind = 7, sgn * exp10(toInt16(h % 40) - 20),
        dec) AS v64,
    multiIf(
        kind = 0, reinterpretAsFloat32(bitOr(bitAnd(h32, 0x807FFFFF), 0x7F800001)),
        kind = 3, reinterpretAsFloat32(bitAnd(h32, 0x807FFFFF)),
        kind = 4, toFloat32(sgn * 1.17549435e-38),
        kind = 5, toFloat32(sgn * 3.4028235e38),
        toFloat32(v64)) AS v32
SELECT number + 60000, v64, v64, v64, v64, v32, v32, v32, v32 FROM numbers(20000);

-- Monotonic values with few fractional parts -> narrow ranges and small RD dictionaries.
INSERT INTO alp_random
WITH
    (SELECT seed FROM alp_random_seed) AS seed,
    sipHash64(number, seed) AS h,
    intDiv(number, 128) + (h % 4) / 4 AS v64,
    toFloat32(v64) AS v32
SELECT number + 80000, v64, v64, v64, v64, v32, v32, v32, v32 FROM numbers(20000);

SELECT count(), countIf(mismatch) FROM alp_random_check;
SELECT * FROM alp_random_check WHERE mismatch ORDER BY i LIMIT 10;

-- The merge re-compresses everything with another block size.
OPTIMIZE TABLE alp_random FINAL;
SELECT count(), countIf(mismatch) FROM alp_random_check;
SELECT * FROM alp_random_check WHERE mismatch ORDER BY i LIMIT 10;

DROP VIEW alp_random_check;
DROP TABLE alp_random;
DROP TABLE alp_random_seed;
