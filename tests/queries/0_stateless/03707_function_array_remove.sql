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

SELECT arrayRemove([1,2,3,2], toUInt8(2*1));
SELECT arrayRemove([1,2,3,2], CAST(2*1 AS UInt8));

SELECT arrayRemove([NULL], NULL);
SELECT arrayRemove([1, NULL, 2], NULL);
SELECT arrayRemove([NULL, NULL, 1], NULL);

SELECT arrayRemove([1, NULL, 2], 1);
SELECT arrayRemove([1, NULL, 2], 2);
SELECT arrayRemove([1, NULL, 2], 3);

SELECT arrayRemove([1, 1, NULL, NULL, nan, nan, 2, 2, 2], NULL);
SELECT arrayRemove([1, 1, NULL, NULL, nan, nan, 2, 2, 2], nan);
SELECT arrayRemove([1, 1, NULL, NULL, nan, nan, 2, 2, 2], CAST(2 AS Float64));

SELECT arrayRemove(arrayMap(x -> 0, [NULL]), 0);
SELECT toString(arrayRemove(arrayMap(x -> 0, [NULL]), 0));

SELECT arrayRemove(['a','b','a'], 'a');

SELECT arrayRemove(['hello', 'world'], concat('wor', 'ld'));
SELECT arrayRemove(['foo', 'bar', 'foo'], repeat('f',1) || 'oo');

SELECT arrayRemove([[[]], [[], []], [[], []], [[]]], [[]]);
SELECT arrayRemove([[1], [1,2], [2,3], [1,2]], [1,2]);
SELECT arrayRemove([[1], [1,2], [2,3], [1,2]], [3]);
