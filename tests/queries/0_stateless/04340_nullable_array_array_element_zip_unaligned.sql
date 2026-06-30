-- Tags: no-random-settings

SET allow_experimental_nullable_array_type = 1;

WITH arrayElementOrNull(CAST([[1]] AS Nullable(Array(Array(UInt8)))), 2) AS x
SELECT throwIf(toTypeName(x) != 'Nullable(Array(UInt8))' OR NOT isNull(x))
FORMAT Null;

WITH arrayElementOrNull(CAST(NULL AS Nullable(Array(Array(UInt8)))), 1) AS x
SELECT throwIf(toTypeName(x) != 'Nullable(Array(UInt8))' OR NOT isNull(x))
FORMAT Null;

WITH arrayElementOrNull(CAST([[1]] AS Nullable(Array(Array(UInt8)))), 1) AS x
SELECT throwIf(x != [1])
FORMAT Null;

WITH arrayZipUnaligned(
    CAST([[1]] AS Nullable(Array(Array(UInt8)))),
    CAST([[2], [3]] AS Nullable(Array(Array(UInt8))))) AS z
SELECT throwIf(toTypeName(z) != 'Nullable(Array(Tuple(Nullable(Array(UInt8)), Nullable(Array(UInt8)))))')
FORMAT Null;

WITH arrayZipUnaligned(
    CAST([[1]] AS Nullable(Array(Array(UInt8)))),
    CAST([[2], [3]] AS Nullable(Array(Array(UInt8))))) AS z
SELECT throwIf(
    tupleElement(arrayElement(z, 1), 1) != [1]
    OR tupleElement(arrayElement(z, 1), 2) != [2]
    OR NOT isNull(tupleElement(arrayElement(z, 2), 1))
    OR tupleElement(arrayElement(z, 2), 2) != [3])
FORMAT Null;

WITH arrayZipUnaligned(
    CAST(NULL AS Nullable(Array(Array(UInt8)))),
    CAST([[2]] AS Nullable(Array(Array(UInt8))))) AS z
SELECT throwIf(NOT isNull(z))
FORMAT Null;

SELECT 'ok';
