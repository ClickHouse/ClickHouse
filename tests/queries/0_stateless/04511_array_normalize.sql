-- Tests that L1Normalize, L2Normalize, LinfNormalize and LpNormalize work for arrays, not only tuples.
-- https://github.com/ClickHouse/ClickHouse/issues/110042

-- Basic usage with arrays (the reported case).
-- `L1Normalize`, `L2Normalize` and `LinfNormalize` are deterministic across platforms (only sum/`sqrt`/max and division),
-- but `LpNormalize` uses `pow`, which is not correctly rounded and differs by a ULP between libm implementations,
-- so its results are rounded to keep the reference stable.
SELECT L2Normalize([1, 2]);
SELECT L1Normalize([1, 2]);
SELECT LinfNormalize([3, 4]);
SELECT arrayMap(x -> round(x, 10), LpNormalize([3, 4], 5));

-- The result of the array and the tuple forms must agree (rounded to absorb ULP differences between the two code paths).
SELECT arrayMap(x -> round(x, 10), L1Normalize([1, 2])) = arrayMap(x -> round(x, 10), [L1Normalize((1, 2)).1, L1Normalize((1, 2)).2]);
SELECT arrayMap(x -> round(x, 10), L2Normalize([3, 4])) = arrayMap(x -> round(x, 10), [L2Normalize((3, 4)).1, L2Normalize((3, 4)).2]);
SELECT arrayMap(x -> round(x, 10), LinfNormalize([3, 4])) = arrayMap(x -> round(x, 10), [LinfNormalize((3, 4)).1, LinfNormalize((3, 4)).2]);
SELECT arrayMap(x -> round(x, 10), LpNormalize([3, 4], 5.)) = arrayMap(x -> round(x, 10), [LpNormalize((3, 4), 5.).1, LpNormalize((3, 4), 5.).2]);

-- Aliases.
SELECT normalizeL1([1, 2]), normalizeL2([3, 4]), normalizeLinf([3, 4]), arrayMap(x -> round(x, 10), normalizeLp([3, 4], 2.));

-- Different nested types. Float32 and BFloat16 arrays produce Float32 arrays, everything else produces Float64 arrays.
SELECT toTypeName(L2Normalize([1, 2, 3]));
SELECT toTypeName(L2Normalize([1.0::Float32, 2.0::Float32]));
SELECT toTypeName(L2Normalize([1.0::Float64, 2.0::Float64]));
SELECT L2Normalize([3.0::Float32, 4.0::Float32]);

-- BFloat16 arrays normalize to Array(Float32), just like Float32 arrays.
SET allow_experimental_bfloat16_type = 1;
SELECT toTypeName(L2Normalize([3.0::BFloat16, 4.0::BFloat16]));
SELECT toTypeName(LpNormalize([3.0::BFloat16, 4.0::BFloat16], 3.));
SELECT L2Normalize([3.0::BFloat16, 4.0::BFloat16]);

-- Non-constant argument.
SELECT L2Normalize(materialize([3, 4]));
SELECT arrayMap(x -> round(x, 10), LpNormalize(materialize([3, 4]), 2.));

-- Arrays stored in a table with several lengths.
DROP TABLE IF EXISTS vec_normalize;
CREATE TABLE vec_normalize (id UInt64, v Array(Float64)) ENGINE = Memory;
INSERT INTO vec_normalize VALUES (1, [3, 4]), (2, [1, 2, 2]), (3, [0, 0, 5]);
SELECT id, L1Normalize(v), L2Normalize(v), LinfNormalize(v), arrayMap(x -> round(x, 10), LpNormalize(v, 3.)) FROM vec_normalize ORDER BY id;
DROP TABLE vec_normalize;

-- Empty array.
SELECT L2Normalize(CAST([], 'Array(Float64)'));

-- Error cases mirror the array norm functions.
SELECT LpNormalize([1, 2]); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT LpNormalize([1, 2], -3.4); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT LpNormalize([1, 2], 'aa'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
-- A non-constant `p` is rejected identically by the tuple and array carriers, so the tuple-or-array
-- surface does not drift: `p` is declared always-constant, so a materialized `p` is rejected before
-- the kernel runs regardless of the carrier.
SELECT LpNormalize([1, 2], materialize(3.14)); -- { serverError ILLEGAL_COLUMN }
SELECT LpNormalize((1, 2), materialize(3.14)); -- { serverError ILLEGAL_COLUMN }
SELECT LpNorm([1, 2], materialize(3.14)); -- { serverError ILLEGAL_COLUMN }
SELECT LpNorm((1, 2), materialize(3.14)); -- { serverError ILLEGAL_COLUMN }
SELECT LpDistance([1, 2], [3, 4], materialize(3.14)); -- { serverError ILLEGAL_COLUMN }
SELECT LpDistance((1, 2), (3, 4), materialize(3.14)); -- { serverError ILLEGAL_COLUMN }

-- `p` must be within `[1, inf)`, so non-finite `p` is rejected instead of silently producing `NaN`s.
SELECT LpNormalize([1, 2], nan); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT LpNormalize([1, 2], inf); -- { serverError ARGUMENT_OUT_OF_BOUND }
-- The `LpNorm` array sibling shares the same validation, so it rejects non-finite `p` too.
SELECT LpNorm([1, 2], nan); -- { serverError ARGUMENT_OUT_OF_BOUND }

-- The tuple carriers dispatched by the same `TupleOrArrayFunction` surface share the validation.
SELECT LpNormalize((1, 2), nan); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT LpNormalize((1, 2), inf); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT LpNorm((1, 2), nan); -- { serverError ARGUMENT_OUT_OF_BOUND }
-- `LpDistance` goes through the same `p` validation for tuples and arrays alike.
SELECT LpDistance((1, 2), (3, 4), nan); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT LpDistance([1, 2], [3, 4], nan); -- { serverError ARGUMENT_OUT_OF_BOUND }

-- Integer `p` (including signed integer types) is accepted consistently by the tuple and array carriers
-- and behaves like the equivalent floating-point `p`.
SELECT LpNormalize([1, 2], toInt8(2)) = LpNormalize([1, 2], 2.);
SELECT LpNormalize((1, 2), toInt8(2)) = LpNormalize((1, 2), 2.);
SELECT LpNorm([1, 2], toInt8(3)) = LpNorm([1, 2], 3.);
SELECT LpNorm((1, 2), toInt8(3)) = LpNorm((1, 2), 3.);
SELECT LpDistance([1, 2], [3, 4], toInt8(3)) = LpDistance([1, 2], [3, 4], 3.);
SELECT LpDistance((1, 2), (3, 4), toInt8(3)) = LpDistance((1, 2), (3, 4), 3.);
-- A negative integer `p` is out of the valid range `[1, inf)`.
SELECT LpNormalize([1, 2], toInt8(-2)); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT LpNormalize((1, 2), toInt8(-2)); -- { serverError ARGUMENT_OUT_OF_BOUND }
