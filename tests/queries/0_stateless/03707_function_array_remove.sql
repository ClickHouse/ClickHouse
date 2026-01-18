SELECT arrayRemove([], 1);

SELECT arrayRemove([0], 0);
SELECT arrayRemove([1], 1);
SELECT arrayRemove([2], 2);

SELECT arrayRemove([1,1], 1);
SELECT arrayRemove([1,2], 1);
SELECT arrayRemove([1,1,2], 1);
SELECT arrayRemove([1,2,1], 1);
SELECT arrayRemove([2,1,1], 1);

SELECT arrayRemove([1,2,2,3,3,3,4,4,4,4,5,5,5,5,5], 2);
SELECT arrayRemove([1,2,2,3,3,3,4,4,4,4,5,5,5,5,5], 3);
SELECT arrayRemove([1,2,2,3,3,3,4,4,4,4,5,5,5,5,5], 6);

SELECT arrayRemove([1,2,3,2], 2*1);

SELECT arrayRemove([NULL], NULL);
SELECT arrayRemove([1, NULL, 2], NULL);
SELECT arrayRemove([NULL, NULL, 1], NULL);

SELECT arrayRemove([1, NULL, 2], 1);
SELECT arrayRemove([1, NULL, 2], 2);
SELECT arrayRemove([1, NULL, 2], 3);

SELECT arrayRemove([1, 1, NULL, NULL, nan, nan, 2, 2, 2], NULL);
SELECT arrayRemove([1, 1, NULL, NULL, nan, nan, 2, 2, 2], nan);
SELECT arrayRemove([1, 1, NULL, NULL, nan, nan, 2, 2, 2], 2);

SELECT arrayRemove(arrayMap(x -> 0, [NULL]), 0);
SELECT toString(arrayRemove(arrayMap(x -> 0, [NULL]), 0));

SELECT arrayRemove(['a','b','a'], 'a');

SELECT arrayRemove(['hello', 'world'], concat('wor', 'ld'));
SELECT arrayRemove(['foo', 'bar', 'foo'], repeat('f',1) || 'oo');

SELECT arrayRemove([[[]], [[], []], [[], []], [[]]], [[]]);
SELECT arrayRemove([[1], [1,2], [2,3], [1,2]], [1,2]);
SELECT arrayRemove([[1], [1,2], [2,3], [1,2]], [3]);

CREATE TABLE test (array Array(UInt32), element UInt32) engine=Memory;
INSERT INTO test VALUES ([1, 2, 3, 2], 2), ([3, 4, 3, 5], 3), ([6, 7, 7, 8], 7);
SELECT arrayRemove(array, element) from test;

SELECT arrayRemove([(1,2), (3,4)], (1,2));

SELECT arrayRemove([1,2,3]); -- {serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}
SELECT arrayRemove([1,2,3], 2, 3); -- {serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}

SELECT arrayRemove(1, 1); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT arrayRemove([1,2,3], [1]); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}

SELECT arrayRemove([1,2,3], '1'); -- {serverError NO_COMMON_TYPE}
SELECT arrayRemove(['a', 'b', 'c'], 1); -- {serverError NO_COMMON_TYPE}

SELECT arrayRemove(CAST(['a', NULL, 'b'] AS Array(LowCardinality(Nullable(String)))), 'a');
SELECT arrayRemove(CAST(['a', NULL, 'b'] AS Array(LowCardinality(Nullable(String)))), NULL);

SELECT arrayRemove(
    [CAST(1 AS Dynamic), CAST(NULL AS Dynamic), CAST(2 AS Dynamic)],
    1
);
SELECT arrayRemove(
    [CAST(1 AS Dynamic), CAST(NULL AS Dynamic), CAST(2 AS Dynamic)],
    NULL
);

SELECT arrayRemove(
    [
        1::Variant(UInt8, String),
        'x'::Variant(UInt8, String),
        NULL::Variant(UInt8, String)
    ],
    'x'::Variant(UInt8, String)
);
SELECT arrayRemove(
    [
        1::Variant(UInt8, String),
        'x'::Variant(UInt8, String),
        NULL::Variant(UInt8, String)
    ],
    NULL
);

CREATE TABLE arr_test (arr Array(Int32)) ENGINE = Memory;
INSERT INTO arr_test VALUES ([1, 2, 3]), ([3, 4, 5]);
SELECT arrayRemove(arr, 3) FROM arr_test;

CREATE TABLE elem_test (arr Array(Int32), elem Nullable(Int32)) ENGINE = Memory;
INSERT INTO elem_test VALUES ([1,2,3], 2), ([1,2,3], NULL), ([1,2,3], 1);
SELECT arrayRemove(arr, elem) FROM elem_test;

CREATE TABLE nullable_arr (arr Array(Nullable(Int32))) ENGINE = Memory;
INSERT INTO nullable_arr VALUES ([1,2,3]), ([NULL,2,3]);
SELECT arrayRemove(arr, 2) FROM nullable_arr;
SELECT arrayRemove(arr, NULL) FROM nullable_arr;

SELECT arrayRemove(arr, elem)
FROM (SELECT [1,2,3] AS arr, number AS elem FROM numbers(3));
