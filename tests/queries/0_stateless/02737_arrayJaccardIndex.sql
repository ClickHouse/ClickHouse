SELECT 'negative tests';

SELECT 'a' AS arr1, 2 AS arr2, round(arrayJaccardIndex(arr1, arr2), 2); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT [] AS arr1, [] AS arr2, round(arrayJaccardIndex(arr1, arr2), 2); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT ['1', '2'] AS arr1, [1,2] AS arr2, round(arrayJaccardIndex(arr1, arr2), 2); -- { serverError NO_COMMON_TYPE }

SELECT 'const arguments';

SELECT [1,2] AS arr1, [1,2,3,4] AS arr2, round(arrayJaccardIndex(arr1, arr2), 2);
SELECT [1, 1.1, 2.2] AS arr1, [2.2, 3.3, 444] AS arr2, round(arrayJaccardIndex(arr1, arr2), 2);
SELECT [toUInt16(1)] AS arr1, [toUInt32(1)] AS arr2, round(arrayJaccardIndex(arr1, arr2), 2);
SELECT ['a'] AS arr1, ['a', 'aa', 'aaa'] AS arr2, round(arrayJaccardIndex(arr1, arr2), 2);
SELECT [[1,2], [3,4]] AS arr1, [[1,2], [3,5]] AS arr2, round(arrayJaccardIndex(arr1, arr2), 2);

SELECT 'non-const arguments';

DROP TABLE IF EXISTS array_jaccard_index;

CREATE TABLE array_jaccard_index (arr Array(UInt8)) engine = MergeTree ORDER BY arr;
INSERT INTO array_jaccard_index values ([1,2,3]);
INSERT INTO array_jaccard_index values ([1,2]);
INSERT INTO array_jaccard_index values ([1]);

SELECT arr, [1,2] AS other, round(arrayJaccardIndex(arr, other), 2) FROM array_jaccard_index ORDER BY arr;
SELECT arr, [] AS other, round(arrayJaccardIndex(arr, other), 2) FROM array_jaccard_index ORDER BY arr;
SELECT [1,2] AS other, arr, round(arrayJaccardIndex(other, arr), 2) FROM array_jaccard_index ORDER BY arr;
SELECT [] AS other, arr,  round(arrayJaccardIndex(other, arr), 2) FROM array_jaccard_index ORDER BY arr;
SELECT arr, arr, round(arrayJaccardIndex(arr, arr), 2) FROM array_jaccard_index ORDER BY arr;

DROP TABLE array_jaccard_index;
