SELECT arrayIntersect(['a', 'b', 'c'], ['a', 'a']);
SELECT arrayIntersect([1, 1], [2, 2]);
SELECT arrayIntersect([1, 1], [1, 2]);
SELECT arrayIntersect([1, 1, 1], [3], [2, 2, 2]);
SELECT arrayIntersect([1, 2], [1, 2], [2]);
SELECT arrayIntersect([1, 1], [2, 1], [2, 2], [1]);
SELECT arrayIntersect([]);
SELECT arrayIntersect([1, 2, 3]);
SELECT arrayIntersect([1, 1], [2, 1], [2, 2], [2, 2, 2]);
