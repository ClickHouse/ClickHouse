-- Tags: no-random-settings

SET allow_experimental_nullable_array_type = 1;

-- Valid Nullable(Array) columns must still be constructable and usable.
SELECT throwIf(NOT isNull(CAST(NULL AS Nullable(Array(UInt8)))))
FORMAT Null;

SELECT throwIf(length(CAST([1, 2, 3], 'Nullable(Array(UInt8))')) != 3)
FORMAT Null;

SELECT throwIf(isNull(length(CAST(NULL AS Nullable(Array(UInt8))))))
FORMAT Null;

SELECT throwIf(arrayPopBack(CAST([1, 2, 3], 'Nullable(Array(UInt8))')) != [1, 2])
FORMAT Null;
