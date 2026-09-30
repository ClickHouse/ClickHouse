-- Mixing constant and non-constant coordinate arguments must not crash with a
-- LOGICAL_ERROR (the dispatch used to look at the runtime column class, which is
-- ColumnConst for the const arguments, and misclassified Float32 as Float64).
SELECT geohashesInBox(CAST(0. AS Float32), materialize(CAST(0. AS Float32)), CAST(1. AS Float32), CAST(1. AS Float32), 1);
SELECT geohashesInBox(CAST(0. AS Float64), materialize(CAST(0. AS Float64)), CAST(1. AS Float64), CAST(1. AS Float64), 1);
SELECT geohashesInBox(materialize(CAST(0. AS Float32)), CAST(0. AS Float32), materialize(CAST(1. AS Float32)), CAST(1. AS Float32), materialize(toUInt8(1)));

-- All-constant and all-non-constant paths still work.
SELECT geohashesInBox(CAST(0. AS Float32), CAST(0. AS Float32), CAST(1. AS Float32), CAST(1. AS Float32), 1);
SELECT geohashesInBox(materialize(CAST(0. AS Float32)), materialize(CAST(0. AS Float32)), materialize(CAST(1. AS Float32)), materialize(CAST(1. AS Float32)), materialize(toUInt8(1)));

-- BFloat16 is not a supported coordinate type and must be rejected with a clean
-- ILLEGAL_TYPE_OF_ARGUMENT, not a LOGICAL_ERROR.
SELECT geohashesInBox(CAST(0. AS BFloat16), CAST(0. AS BFloat16), CAST(1. AS BFloat16), CAST(1. AS BFloat16), 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT geohashesInBox(CAST(0. AS BFloat16), materialize(CAST(0. AS BFloat16)), CAST(1. AS BFloat16), CAST(1. AS BFloat16), 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Coordinate arguments must all share the same type.
SELECT geohashesInBox(CAST(0. AS Float32), CAST(0. AS Float64), CAST(1. AS Float32), CAST(1. AS Float64), 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
