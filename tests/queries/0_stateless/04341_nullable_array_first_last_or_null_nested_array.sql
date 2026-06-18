-- Tags: no-random-settings

SET allow_experimental_nullable_array_type = 1;

WITH arrayFirstOrNull(x -> 1, CAST([[1], [2]] AS Nullable(Array(Array(UInt8))))) AS x
SELECT throwIf(toTypeName(x) != 'Nullable(Array(UInt8))' OR x != [1])
FORMAT Null;

WITH arrayLastOrNull(x -> 1, CAST([[1], [2]] AS Nullable(Array(Array(UInt8))))) AS x
SELECT throwIf(toTypeName(x) != 'Nullable(Array(UInt8))' OR x != [2])
FORMAT Null;

WITH arrayFirstOrNull(x -> 0, CAST([[1], [2]] AS Nullable(Array(Array(UInt8))))) AS x
SELECT throwIf(toTypeName(x) != 'Nullable(Array(UInt8))' OR NOT isNull(x))
FORMAT Null;

WITH arrayLastOrNull(x -> 0, CAST([[1], [2]] AS Nullable(Array(Array(UInt8))))) AS x
SELECT throwIf(toTypeName(x) != 'Nullable(Array(UInt8))' OR NOT isNull(x))
FORMAT Null;

WITH arrayFirstOrNull(x -> 1, CAST(NULL AS Nullable(Array(Array(UInt8))))) AS x
SELECT throwIf(toTypeName(x) != 'Nullable(Array(UInt8))' OR NOT isNull(x))
FORMAT Null;

WITH arrayLastOrNull(x -> 1, CAST(NULL AS Nullable(Array(Array(UInt8))))) AS x
SELECT throwIf(toTypeName(x) != 'Nullable(Array(UInt8))' OR NOT isNull(x))
FORMAT Null;

SELECT 'ok';
