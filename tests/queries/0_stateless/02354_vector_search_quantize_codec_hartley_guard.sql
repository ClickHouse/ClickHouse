-- Regression test for the vector-search codec's compatibility carve-out around the order-9 Hartley small factor.
--
-- `randomHadamardTransform` now has an exact non-padding path for dimensions of the form 2^k * 9 (e.g. 1152 = 128 * 9),
-- implemented via the shared `HadamardTransform::kroneckerFactorFor` helper. The `Quantized(...)` codec deliberately does
-- NOT use that Hartley factor: `VectorQuantizer.cpp`'s `hadamardFactorFor` wrapper filters `kroneckerFactorFor` down to
-- `SmallFactorKind::Hadamard`, so a 1152-dimensional vector keeps the OLD "zero-pad to the next power of two (2048)"
-- projection. Switching it to the exact Hartley path would silently rewrite every already-stored `Quantized(..., 1152)`
-- code, which is a backward-incompatible on-disk change.
--
-- This pins that contract with an invariant that actually distinguishes the padded-2048 path from the exact-1152 path
-- (a plain encode/query round-trip cannot: if both the data and query sides switched to Hartley together they would still
-- agree). For the padded path, encoding a 1152-dim vector produces exactly the same structured projection as encoding the
-- same vector zero-padded to 2048 dims: identical sign-flip diagonals (same seed, same working dimension 2048) and the
-- identical FWHT over 2048. So the 1152/8 = 144 packed sign bytes of a `rabitq` code at 1152 dims must be byte-for-byte
-- equal to the leading 144 sign bytes of the code at 2048 dims. The exact Hartley path (working dimension 1152, a dense
-- order-9 rotation) would produce different signs and break this equality. The comparison is between two codes produced by
-- the same binary, so it is robust to per-platform floating-point differences (no absolute byte values are pinned).
--
-- The codec is gated behind `allow_experimental_codecs`.

SET allow_experimental_codecs = 1;

DROP TABLE IF EXISTS quantize_1152;
DROP TABLE IF EXISTS quantize_2048;

CREATE TABLE quantize_1152
(
    id UInt32,
    vec Array(Float32) CODEC(Quantized('rabitq', 1152))
)
ENGINE = MergeTree ORDER BY id;

CREATE TABLE quantize_2048
(
    id UInt32,
    vec Array(Float32) CODEC(Quantized('rabitq', 2048))
)
ENGINE = MergeTree ORDER BY id;

-- The same base vector (1152 pseudo-random coordinates); the 2048 table stores it zero-padded to 2048 dims.
INSERT INTO quantize_1152 (id, vec)
SELECT number, arrayMap(j -> toFloat32(sipHash64(number, j) % 2000 / 1000.0 - 1.0), range(1152))
FROM numbers(20);

INSERT INTO quantize_2048 (id, vec)
SELECT number, arrayResize(arrayMap(j -> toFloat32(sipHash64(number, j) % 2000 / 1000.0 - 1.0), range(1152)), 2048)
FROM numbers(20);

-- The 1152-dim rabitq code is 1152 / 8 = 144 packed sign bytes plus the 4-byte inv_factor.
SELECT 'code_length_1152', length(vec.quantized) FROM quantize_1152 GROUP BY length(vec.quantized);

-- Contract: the 144 sign bytes of the 1152 code equal the leading 144 sign bytes of the 2048 (zero-padded) code, proving
-- the codec still zero-pads 1152 to 2048 rather than taking the exact order-9 Hartley path. If the codec silently started
-- using `kroneckerFactorFor` directly, the signs would differ and this would return 0.
-- The `vec.quantized` subcolumn is read directly from each table (it does not survive a `SELECT id, vec` subquery
-- boundary under the old analyzer), then compared per id via a join, so the result does not depend on aggregation order.
SELECT 'signs_match_padded_2048', min(a.code = b.code)
FROM (SELECT id, hex(substring(vec.quantized, 1, 144)) AS code FROM quantize_1152) AS a
ALL INNER JOIN (SELECT id, hex(substring(vec.quantized, 1, 144)) AS code FROM quantize_2048) AS b USING (id);

DROP TABLE quantize_1152;
DROP TABLE quantize_2048;
