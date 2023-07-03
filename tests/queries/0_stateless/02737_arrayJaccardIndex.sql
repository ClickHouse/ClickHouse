SELECT ['a'] AS arr_1, ['a', 'aa', 'aaa'] AS arr_2, round(arrayJaccardIndex(arr_1, arr_2), 2);
SELECT [1, 1.1, 2.2] AS arr_1, [2.2, 3.3, 444] AS arr_2, round(arrayJaccardIndex(arr_1, arr_2), 2);
SELECT [toUInt16(1)] AS arr_1, [toUInt32(1)] AS arr_2, round(arrayJaccardIndex(arr_1, arr_2), 2);
SELECT [1,2] AS arr_1, [1,2,3,4] AS arr_2, round(arrayJaccardIndex(arr_1, arr_2), 2);
SELECT [[1,2], [3,4]] AS arr_1, [[1,2], [3,5]] AS arr_2, round(arrayJaccardIndex(arr_1, arr_2), 2);

DROP TABLE IF EXISTS array_jaccard_index;

CREATE TABLE array_jaccard_index (arr Array(UInt8)) engine = MergeTree ORDER BY arr;
INSERT INTO array_jaccard_index values ([1,2,3]);
INSERT INTO array_jaccard_index values ([1,2]);
INSERT INTO array_jaccard_index values ([1]);

SELECT arr AS arr_1, [1,2] AS arr_2, round(arrayJaccardIndex(arr_1, arr_2), 2) FROM array_jaccard_index ORDER BY arr;
SELECT arr AS arr_1, [] AS arr_2, round(arrayJaccardIndex(arr_1, arr_2), 2) FROM array_jaccard_index ORDER BY arr;
SELECT [] AS arr_1, arr AS arr_2,  round(arrayJaccardIndex(arr_1, arr_2), 2) FROM array_jaccard_index ORDER BY arr;
SELECT [1,2] AS arr_1, arr AS arr_2, round(arrayJaccardIndex(arr_1, arr_2), 2) FROM array_jaccard_index ORDER BY arr;
SELECT arr AS arr_1, arr AS arr_2, round(arrayJaccardIndex(arr_1, arr_2), 2) FROM array_jaccard_index ORDER BY arr;

drop table array_jaccard_index;
