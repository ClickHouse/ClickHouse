SET allow_experimental_analyzer = 1;

SELECT number FROM numbers(10) WHERE has([number % 3, number % 5], number % 2) ORDER BY number;
SELECT '-- IN --';
SELECT number FROM numbers(10) WHERE number % 2 IN [number % 3, number % 5] ORDER BY number SETTINGS allow_experimental_analyzer = 1;
SELECT number FROM numbers(10) WHERE number % 2 IN [number % 3, number % 5] ORDER BY number SETTINGS allow_experimental_analyzer = 0; -- { serverError UNKNOWN_IDENTIFIER }

SELECT '-- MORE CASES --';

-- { echoOn }

SELECT (1, 2) in [number % 3, number % 5] FROM numbers(2); -- { serverError NO_COMMON_TYPE }
SELECT (1, 2) in (SELECT [0, 0] UNION ALL SELECT [1, 1]); -- { serverError TYPE_MISMATCH }

SELECT (1, 2) in [(number % 3, number % 5)] FROM numbers(2);
SELECT (1, 2) in (SELECT (0, 0)), (1, 2) in (SELECT (1, 1));

SELECT (1, 1) in [(number % 3, number % 5)] FROM numbers(2);
SELECT (1, 1) in (SELECT (0, 0)), (1, 1) in (SELECT (1, 1));

SELECT (1, null) in [(number % 3, number % 5)] FROM numbers(2);
SELECT (1, null) in (SELECT (0, 0::Nullable(Int))), (1, null) in (SELECT (1, 1::Nullable(Int)));

SELECT (1, null) in [(number % 3, number % 5), (1, null)] FROM numbers(2);
SELECT (1, null) in (SELECT (0, 0::Nullable(Int)) UNION ALL SELECT (1, null)), (1, null) in (SELECT (1, 1::Nullable(Int)) UNION ALL SELECT (1, null));

SELECT 'ANOTHER SETTING';

set transform_null_in = 1;

SELECT (1, 2) in [number % 3, number % 5] FROM numbers(2); -- { serverError NO_COMMON_TYPE }
SELECT (1, 2) in (SELECT [0, 0] UNION ALL SELECT [1, 1]); -- { serverError TYPE_MISMATCH }

SELECT (1, 2) in [(number % 3, number % 5)] FROM numbers(2);
SELECT (1, 2) in (SELECT (0, 0)), (1, 2) in (SELECT (1, 1));

SELECT (1, 1) in [(number % 3, number % 5)] FROM numbers(2);
SELECT (1, 1) in (SELECT (0, 0)), (1, 1) in (SELECT (1, 1));

SELECT (1, null) in [(number % 3, number % 5)] FROM numbers(2);
SELECT (1, null) in (SELECT (0, 0::Nullable(Int))), (1, null) in (SELECT (1, 1::Nullable(Int)));

SELECT (1, null) in [(number % 3, number % 5), (1, null)] FROM numbers(2);
SELECT (1, null) in (SELECT (0, 0::Nullable(Int)) UNION ALL SELECT (1, null)), (1, null) in (SELECT (1, 1::Nullable(Int)) UNION ALL SELECT (1, null));

--- with tuple rewritten into array
SELECT *
FROM numbers(1000)
WHERE number IN (123, 10 - number, 456);

-- Consistency of transform_null_in to non-const arguments
SELECT  
    NULL IN (1, number),  
    NULL IN (1, number, NULL),  
    NULL IN (1, 2),  
    NULL IN (1, NULL)  
FROM numbers(1)  
SETTINGS transform_null_in = 1;

SELECT  
    NULL IN (1, number),  
    NULL IN (1, number, NULL),  
    NULL IN (1, 2),  
    NULL IN (1, NULL)  
FROM numbers(1)  
SETTINGS transform_null_in = 0;

-- Consistency for arrays/tuples
SELECT toNullable(1) IN [1, number]  
FROM numbers(2);

SELECT toNullable(1) IN (1, number)  
FROM numbers(2);

-- Common type consistency
SELECT 'a' IN (5, number, 'a')  
FROM numbers(2);

SELECT
    NULL IN (
      258,
      CAST('string' AS Nullable(String)),
      CAST(number   AS Nullable(UInt64))
    )
FROM numbers(1)
SETTINGS transform_null_in = 1;

SELECT
    NULL IN (
      258,
      CAST('string' AS Nullable(String)),
      CAST(number   AS Nullable(UInt64))
    )
FROM numbers(1)
SETTINGS transform_null_in = 0;

SELECT
    NULL IN (
      258,
      CAST('string' AS Nullable(String)),
      CAST(number   AS Nullable(UInt64)),
      NULL
    )
FROM numbers(1)
SETTINGS transform_null_in = 1;

SELECT
    NULL IN (
      258,
      CAST('string' AS Nullable(String)),
      CAST(number   AS Nullable(UInt64)),
      NULL
    )
FROM numbers(1)
SETTINGS transform_null_in = 0;

SELECT NULL IN [1, number]
FROM numbers(1)
SETTINGS transform_null_in = 1;

SELECT NULL IN [1, number]
FROM numbers(1)
SETTINGS transform_null_in = 0;

SELECT 1 IN [1, toNullable(number)]
FROM numbers(2)
SETTINGS transform_null_in = 1;

SELECT 1 IN [1, toNullable(number)]
FROM numbers(2)
SETTINGS transform_null_in = 0;

SELECT
    0 AS x,
    [if(number > 1, NULL, number)] AS arr,
    tuple(arr[1]) AS t,
    x IN (t),
    x IN (arr)
FROM numbers(3)
SETTINGS transform_null_in = 0;

SELECT
    arrayJoin([0, 1, NULL]) AS x,
    [if(number > 1, NULL, number)] AS arr,
    tuple(arr[1]) AS t,
    x IN (t),
    x IN (arr)
FROM numbers(3)
SETTINGS transform_null_in = 0;

SELECT
    NULL AS x,
    [if(number > 1, NULL, number)] AS arr,
    tuple(arr[1]) AS t,
    x IN (t),
    x IN (arr)
FROM numbers(3)
SETTINGS transform_null_in = 0;
