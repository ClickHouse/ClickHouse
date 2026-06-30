-- Tags: no-random-settings

SET allow_experimental_nullable_array_type = 1;

SELECT throwIf(toTypeName(dynamicElement(CAST([CAST(toUInt64(1), 'Dynamic')], 'Nullable(Array(Dynamic))'), 'UInt64')) != 'Nullable(Array(Nullable(UInt64)))')
FORMAT Null;

SELECT throwIf(dynamicElement(CAST([CAST(toUInt64(1), 'Dynamic'), CAST('str', 'Dynamic')], 'Nullable(Array(Dynamic))'), 'UInt64') != [1, NULL])
FORMAT Null;

SELECT throwIf(NOT isNull(dynamicElement(CAST(NULL, 'Nullable(Array(Dynamic))'), 'UInt64')))
FORMAT Null;

SELECT throwIf(NOT isNull(res))
FROM
(
    SELECT dynamicElement(a, 'UInt64') AS res
    FROM
    (
        SELECT if(number = 0, CAST(NULL, 'Nullable(Array(Dynamic))'), CAST([CAST(toUInt64(1), 'Dynamic')], 'Nullable(Array(Dynamic))')) AS a
        FROM numbers(2)
    )
    WHERE isNull(a)
)
FORMAT Null;

SELECT throwIf(res != [1])
FROM
(
    SELECT dynamicElement(a, 'UInt64') AS res
    FROM
    (
        SELECT if(number = 0, CAST(NULL, 'Nullable(Array(Dynamic))'), CAST([CAST(toUInt64(1), 'Dynamic')], 'Nullable(Array(Dynamic))')) AS a
        FROM numbers(2)
    )
    WHERE NOT isNull(a)
)
FORMAT Null;

SELECT 'ok';
