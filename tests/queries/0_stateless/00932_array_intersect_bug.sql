SELECT arraySort(arrayIntersect(['a', 'b', 'c'], ['a', 'a']));
SELECT arraySort(arrayIntersect([1, 1], [2, 2]));
SELECT arraySort(arrayIntersect([1, 1], [1, 2]));
SELECT arraySort(arrayIntersect([1, 1, 1], [3], [2, 2, 2]));
SELECT arraySort(arrayIntersect([1, 2], [1, 2], [2]));
SELECT arraySort(arrayIntersect([1, 1], [2, 1], [2, 2], [1]));
SELECT arraySort(arrayIntersect([]));
SELECT arraySort(arrayIntersect([1, 2, 3]));
SELECT arraySort(arrayIntersect([1, 1], [2, 1], [2, 2], [2, 2, 2]));
