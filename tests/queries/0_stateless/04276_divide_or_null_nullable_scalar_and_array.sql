-- Regression: divideOrNull / intDivOrNull must not disable default NULL handling for scalar Nullable(T).
-- Nullable(Array(...)) still needs unde-nested types only for array-involved calls.

SELECT throwIf(toTypeName(divideOrNull(CAST(1 AS Nullable(UInt8)), 2)) != 'Nullable(Float64)')
FORMAT Null;

SELECT throwIf(toTypeName(divideOrNull(1, CAST(2 AS Nullable(UInt8)))) != 'Nullable(Float64)')
FORMAT Null;

SELECT throwIf(divideOrNull(CAST(1 AS Nullable(UInt8)), 2) != 0.5)
FORMAT Null;

SELECT throwIf(divideOrNull(1, CAST(2 AS Nullable(UInt8))) != 0.5)
FORMAT Null;

SELECT throwIf(toTypeName(intDivOrNull(CAST(3 AS Nullable(Int32)), CAST(2 AS Nullable(Int32)))) != 'Nullable(Int32)')
FORMAT Null;

SELECT throwIf(intDivOrNull(CAST(3 AS Nullable(Int32)), CAST(2 AS Nullable(Int32))) != 1)
FORMAT Null;

SET allow_experimental_nullable_array_type = 1;

SELECT throwIf(toTypeName(divideOrNull(CAST([1, 2] AS Nullable(Array(Int8))), 2)) != 'Nullable(Array(Nullable(Float64)))')
FORMAT Null;

SELECT throwIf(divideOrNull(CAST([1, 2] AS Nullable(Array(Int8))), 2) != [0.5, 1])
FORMAT Null;

SELECT throwIf(NOT isNull(divideOrNull(CAST(NULL AS Nullable(Array(Int8))), 2)))
FORMAT Null;

SELECT throwIf(toTypeName(intDivOrNull(CAST([5, 7] AS Nullable(Array(Int32))), 2)) != 'Nullable(Array(Nullable(Int32)))')
FORMAT Null;

SELECT throwIf(intDivOrNull(CAST([5, 7] AS Nullable(Array(Int32))), 2) != [2, 3])
FORMAT Null;
