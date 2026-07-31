-- Tests CAST between two QBit types that differ in the element type and/or the stride (the number of stride groups),
-- while keeping the dimension. Such a cast reconstructs the source vector at its native precision and re-transposes it
-- into the target layout, so it must agree with building the target QBit directly from the reconstructed Array; an
-- element-type-preserving (stride-only) cast must additionally be lossless (it is a pure byte permutation). Changing
-- the dimension is rejected in 03364_qbit_negative.

SELECT 'Element type change (dimension and stride preserved); QBit prints as its reconstructed vector';
SELECT [1, 2, 3, 4]::QBit(Float32, 4)::QBit(Float64, 4);
SELECT [1, 2, 3, 4]::QBit(Float64, 4)::QBit(Float32, 4);
SELECT [1, 2, 3, 4]::QBit(Float32, 4)::QBit(BFloat16, 4);
SELECT [1, 2, 3, 4]::QBit(BFloat16, 4)::QBit(Float32, 4);
SELECT [10, -20, 30, -40]::QBit(Int8, 4)::QBit(Float64, 4);
SELECT [10, -20, 30, -40]::QBit(Float64, 4)::QBit(Int8, 4);

SELECT 'The bit-transposed storage is identical to building the target QBit directly from the vector';
-- QBit is comparable for equality, so this compares the underlying transposed FixedStrings byte for byte.
SELECT [1, 2, 3, 4]::QBit(Float32, 4)::QBit(Float64, 4) = [1, 2, 3, 4]::QBit(Float64, 4);
SELECT [1, 2, 3, 4]::QBit(BFloat16, 4)::QBit(Float32, 4) = [1, 2, 3, 4]::QBit(Float32, 4);
SELECT range(16)::Array(Float32)::QBit(Float32, 16, 8)::QBit(Float32, 16) = range(16)::Array(Float32)::QBit(Float32, 16);
SELECT range(16)::Array(Float32)::QBit(Float32, 16)::QBit(Float32, 16, 8) = range(16)::Array(Float32)::QBit(Float32, 16, 8);
SELECT range(16)::Array(Float32)::QBit(Float32, 16, 8)::QBit(Float64, 16) = range(16)::Array(Float32)::QBit(Float64, 16);

SELECT 'Stride-only change is lossless (same element type); exercises the byte-permutation fast path';
SELECT range(16)::Array(Float32)::QBit(Float32, 16)::QBit(Float32, 16, 8)::Array(Float32) = range(16)::Array(Float32);
SELECT range(16)::Array(Float32)::QBit(Float32, 16, 8)::QBit(Float32, 16)::Array(Float32) = range(16)::Array(Float32);
SELECT range(32)::Array(Float64)::QBit(Float64, 32, 16)::QBit(Float64, 32, 8)::Array(Float64) = range(32)::Array(Float64);
SELECT range(32)::Array(Int8)::QBit(Int8, 32, 8)::QBit(Int8, 32, 16)::Array(Int8) = range(32)::Array(Int8);
SELECT range(64)::Array(BFloat16)::QBit(BFloat16, 64, 8)::QBit(BFloat16, 64, 32)::Array(BFloat16) = range(64)::Array(BFloat16);

SELECT 'Narrowing loses precision exactly like the corresponding Array narrowing';
SELECT [0.1, 0.2, 0.3, 0.4]::QBit(Float64, 4)::QBit(Float32, 4)::Array(Float32) = [0.1, 0.2, 0.3, 0.4]::Array(Float32);
SELECT [0.1, 0.2, 0.3, 0.4]::QBit(Float64, 4)::QBit(BFloat16, 4)::Array(BFloat16) = [0.1, 0.2, 0.3, 0.4]::Array(BFloat16);

SELECT 'Simultaneous element-type and stride change';
SELECT range(16)::Array(Float32)::QBit(Float32, 16, 8)::QBit(Float64, 16)::Array(Float64) = range(16)::Array(Float64);
SELECT range(32)::Array(Float64)::QBit(Float64, 32, 16)::QBit(Float32, 32, 8)::Array(Float32) = range(32)::Array(Float32);

SELECT 'Simultaneous element-type and stride change on the Float32/BFloat16 byte-repack fast path';
-- Unlike the Float32/Float64 block above (which reconstructs and re-transposes), a Float32 <-> BFloat16 change takes the
-- byte-permutation fast path in repackQBit because the two types share their top 16 bit planes; combining it with a stride
-- change exercises that branch's restriding. The result must be byte-for-byte identical to building the target QBit
-- directly. The integers 0..15 are exact in BFloat16, so no low mantissa bit is set: truncating to BFloat16 equals
-- rounding, and Float32's low 16 planes (dropped when narrowing, zero-filled when widening) are all zero either way.
SELECT range(16)::Array(Float32)::QBit(Float32, 16, 8)::QBit(BFloat16, 16) = range(16)::Array(Float32)::QBit(BFloat16, 16);
SELECT range(16)::Array(Float32)::QBit(Float32, 16)::QBit(BFloat16, 16, 8) = range(16)::Array(Float32)::QBit(BFloat16, 16, 8);
SELECT range(16)::Array(Float32)::QBit(BFloat16, 16, 8)::QBit(Float32, 16) = range(16)::Array(Float32)::QBit(Float32, 16);
SELECT range(16)::Array(Float32)::QBit(BFloat16, 16)::QBit(Float32, 16, 8) = range(16)::Array(Float32)::QBit(Float32, 16, 8);

SELECT 'Materialized (non-constant) source column';
SELECT materialize([1, 2, 3, 4]::QBit(Float32, 4))::QBit(Float64, 4)::Array(Float64);
SELECT materialize(range(16)::Array(Float32)::QBit(Float32, 16, 8))::QBit(Float32, 16)::Array(Float32) = range(16)::Array(Float32);

SELECT 'From a table with multiple rows including a default (all-zero) row';
DROP TABLE IF EXISTS qbit_cast_params_test;
CREATE TABLE qbit_cast_params_test (id UInt32, vec QBit(Float32, 16, 8)) ENGINE = Memory;
INSERT INTO qbit_cast_params_test VALUES (1, range(16)), (2, arrayReverse(range(16)));
INSERT INTO qbit_cast_params_test (id) VALUES (3);
SELECT id, CAST(vec AS QBit(Float64, 16))::Array(Float64) FROM qbit_cast_params_test ORDER BY id;
SELECT id, CAST(vec AS QBit(Float32, 16))::Array(Float32) FROM qbit_cast_params_test ORDER BY id;
DROP TABLE qbit_cast_params_test;

SELECT 'Nullable source: NULL stays NULL when the target is also Nullable';
DROP TABLE IF EXISTS qbit_cast_params_nullable;
CREATE TABLE qbit_cast_params_nullable (id UInt32, vec Nullable(QBit(Float32, 4))) ENGINE = Memory;
INSERT INTO qbit_cast_params_nullable VALUES (1, [1, 2, 3, 4]), (2, NULL), (3, [5, 6, 7, 8]);
SELECT id, CAST(vec AS Nullable(QBit(Float64, 4))) IS NULL FROM qbit_cast_params_nullable ORDER BY id;
-- Values on the non-NULL rows are preserved (Nullable target, so a NULL row could never fail the cast).
SELECT id, CAST(vec AS Nullable(QBit(Float64, 4)))::Array(Float64) FROM qbit_cast_params_nullable WHERE vec IS NOT NULL ORDER BY id;
DROP TABLE qbit_cast_params_nullable;

SELECT 'Nullable source with a stride-only change (byte-permutation fast path)';
DROP TABLE IF EXISTS qbit_cast_params_nullable_stride;
CREATE TABLE qbit_cast_params_nullable_stride (id UInt32, vec Nullable(QBit(Float32, 16, 8))) ENGINE = Memory;
INSERT INTO qbit_cast_params_nullable_stride VALUES (1, range(16)), (2, NULL), (3, arrayReverse(range(16)));
SELECT id, CAST(vec AS Nullable(QBit(Float32, 16))) IS NULL FROM qbit_cast_params_nullable_stride ORDER BY id;
SELECT id, CAST(vec AS Nullable(QBit(Float32, 16)))::Array(Float32) FROM qbit_cast_params_nullable_stride WHERE vec IS NOT NULL ORDER BY id;
DROP TABLE qbit_cast_params_nullable_stride;

SELECT 'Casting a NULL to a non-Nullable QBit fails';
SELECT CAST(materialize(CAST(NULL AS Nullable(QBit(Float32, 4)))) AS QBit(Float64, 4)); -- { serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN }

SELECT 'accurateCastOrNull between QBit types';
SELECT accurateCastOrNull([1, 2, 3, 4]::QBit(Float32, 4), 'QBit(Float64, 4)')::Array(Float64);
SELECT accurateCastOrNull([1, 2, 3, 4]::QBit(Float64, 4), 'QBit(Float32, 4)')::Array(Float32);

SELECT 'accurate* narrowing Float32 -> BFloat16 must reject inexact rows (not take the byte-repack fast path)';
-- 0.1 .. 0.4 are not exactly representable in BFloat16. The byte-repack fast path would silently truncate the mantissa,
-- but accurateCastOrNull must null the whole row (and accurateCast must throw) instead, matching the Array narrowing.
SELECT accurateCastOrNull([0.1, 0.2, 0.3, 0.4]::QBit(Float32, 4), 'QBit(BFloat16, 4)') IS NULL;
SELECT accurateCast([0.1, 0.2, 0.3, 0.4]::QBit(Float32, 4), 'QBit(BFloat16, 4)'); -- { serverError CANNOT_CONVERT_TYPE }
-- Exactly-representable values (integers) still convert, and agree with building the target QBit directly.
SELECT accurateCastOrNull([1, 2, 3, 4]::QBit(Float32, 4), 'QBit(BFloat16, 4)')::Array(BFloat16) = [1, 2, 3, 4]::Array(BFloat16);
SELECT accurateCast([1, 2, 3, 4]::QBit(Float32, 4), 'QBit(BFloat16, 4)')::Array(BFloat16) = [1, 2, 3, 4]::Array(BFloat16);
-- A stride change stacked on top of the narrowing rejects inexact rows the same way and preserves exact ones.
SELECT accurateCastOrNull(arrayMap(x -> x / 10, range(16))::Array(Float32)::QBit(Float32, 16, 8), 'QBit(BFloat16, 16)') IS NULL;
SELECT accurateCastOrNull(range(16)::Array(Float32)::QBit(Float32, 16, 8), 'QBit(BFloat16, 16)')::Array(BFloat16) = range(16)::Array(BFloat16);

SELECT 'accurate* widening BFloat16 -> Float32 is always exact (byte-repack fast path stays valid)';
SELECT accurateCastOrNull([0.1, 0.2, 0.3, 0.4]::QBit(BFloat16, 4), 'QBit(Float32, 4)') IS NULL;
SELECT accurateCastOrNull([0.1, 0.2, 0.3, 0.4]::QBit(BFloat16, 4), 'QBit(Float32, 4)')::Array(Float32) = [0.1, 0.2, 0.3, 0.4]::Array(BFloat16)::Array(Float32);

SELECT 'accurate* stride-only change is lossless (byte-permutation fast path stays valid)';
SELECT accurateCastOrNull(range(16)::Array(Float32)::QBit(Float32, 16, 8), 'QBit(Float32, 16)') IS NULL;
SELECT accurateCastOrNull(range(16)::Array(Float32)::QBit(Float32, 16, 8), 'QBit(Float32, 16)')::Array(Float32) = range(16)::Array(Float32);


SELECT 'Same-stride Float32/BFloat16 fast path clears trailing padding bits (dimension not a multiple of 8)';
-- A tuple-backed QBit from a VALUES / IN section is not required to zero the unused padding bits of the final partial
-- byte: convertFieldToType checks only the tuple's element count and each string's length. When the dimension is not a
-- multiple of 8 (here 3, so bits 3..7 of the single byte are padding), the same-stride Float32 <-> BFloat16 byte-repack
-- copies each plane wholesale, so it must clear those bits afterwards to stay canonical -- the reconstruct-and-convert
-- path always leaves them zero and QBit equality compares the raw bytes. Poison bit 3 (a padding bit) of the most
-- significant plane of the source, then check the cast agrees with building the target QBit directly and that the value
-- is unchanged (padding bits carry no dimension, so they never affect the represented vector).
-- Narrowing Float32 -> BFloat16 (the AI's cited QBit(Float32, N) -> QBit(BFloat16, N) case).
SELECT CAST(qbit AS QBit(BFloat16, 3)) = [1, 2, 3]::QBit(BFloat16, 3),
       CAST(qbit AS QBit(BFloat16, 3))::Array(BFloat16) = [1, 2, 3]::Array(BFloat16)
FROM format('Values', 'qbit QBit(Float32, 3)', '(tuple(
    reinterpretAsFixedString(toUInt8(bitOr(reinterpretAsUInt8([1, 2, 3]::QBit(Float32, 3).1), 8))), [1, 2, 3]::QBit(Float32, 3).2, [1, 2, 3]::QBit(Float32, 3).3, [1, 2, 3]::QBit(Float32, 3).4,
    [1, 2, 3]::QBit(Float32, 3).5, [1, 2, 3]::QBit(Float32, 3).6, [1, 2, 3]::QBit(Float32, 3).7, [1, 2, 3]::QBit(Float32, 3).8,
    [1, 2, 3]::QBit(Float32, 3).9, [1, 2, 3]::QBit(Float32, 3).10, [1, 2, 3]::QBit(Float32, 3).11, [1, 2, 3]::QBit(Float32, 3).12,
    [1, 2, 3]::QBit(Float32, 3).13, [1, 2, 3]::QBit(Float32, 3).14, [1, 2, 3]::QBit(Float32, 3).15, [1, 2, 3]::QBit(Float32, 3).16,
    [1, 2, 3]::QBit(Float32, 3).17, [1, 2, 3]::QBit(Float32, 3).18, [1, 2, 3]::QBit(Float32, 3).19, [1, 2, 3]::QBit(Float32, 3).20,
    [1, 2, 3]::QBit(Float32, 3).21, [1, 2, 3]::QBit(Float32, 3).22, [1, 2, 3]::QBit(Float32, 3).23, [1, 2, 3]::QBit(Float32, 3).24,
    [1, 2, 3]::QBit(Float32, 3).25, [1, 2, 3]::QBit(Float32, 3).26, [1, 2, 3]::QBit(Float32, 3).27, [1, 2, 3]::QBit(Float32, 3).28,
    [1, 2, 3]::QBit(Float32, 3).29, [1, 2, 3]::QBit(Float32, 3).30, [1, 2, 3]::QBit(Float32, 3).31, [1, 2, 3]::QBit(Float32, 3).32))');
-- Widening BFloat16 -> Float32 (the shared planes are copied and masked; the extra low planes stay zero).
SELECT CAST(qbit AS QBit(Float32, 3)) = [1, 2, 3]::QBit(Float32, 3),
       CAST(qbit AS QBit(Float32, 3))::Array(Float32) = [1, 2, 3]::Array(Float32)
FROM format('Values', 'qbit QBit(BFloat16, 3)', '(tuple(
    reinterpretAsFixedString(toUInt8(bitOr(reinterpretAsUInt8([1, 2, 3]::QBit(BFloat16, 3).1), 8))), [1, 2, 3]::QBit(BFloat16, 3).2, [1, 2, 3]::QBit(BFloat16, 3).3, [1, 2, 3]::QBit(BFloat16, 3).4,
    [1, 2, 3]::QBit(BFloat16, 3).5, [1, 2, 3]::QBit(BFloat16, 3).6, [1, 2, 3]::QBit(BFloat16, 3).7, [1, 2, 3]::QBit(BFloat16, 3).8,
    [1, 2, 3]::QBit(BFloat16, 3).9, [1, 2, 3]::QBit(BFloat16, 3).10, [1, 2, 3]::QBit(BFloat16, 3).11, [1, 2, 3]::QBit(BFloat16, 3).12,
    [1, 2, 3]::QBit(BFloat16, 3).13, [1, 2, 3]::QBit(BFloat16, 3).14, [1, 2, 3]::QBit(BFloat16, 3).15, [1, 2, 3]::QBit(BFloat16, 3).16))');
