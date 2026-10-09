-- `arrayIntersect` fills its hash map from the smallest argument and looks up the rest, so the
-- result must not depend on the order or on the relative sizes of the arguments.

SELECT '-- the result does not depend on the argument order';
SELECT arraySort(arrayIntersect(range(number * 100), [1, 2, 3, 500], [2, 3, 500, 900])) FROM numbers(6);
SELECT arraySort(arrayIntersect([1, 2, 3, 500], range(number * 100), [2, 3, 500, 900])) FROM numbers(6);
SELECT arraySort(arrayIntersect([2, 3, 500, 900], [1, 2, 3, 500], range(number * 100))) FROM numbers(6);

SELECT '-- a value must be present in every argument, also when it is repeated';
SELECT arrayIntersect([1], [2], [1, 1]);
SELECT arrayIntersect([1, 1], [2], [1]);
SELECT arrayIntersect([1, 2], [2], [1, 1, 2]);
SELECT arrayIntersect([1, 1, 2, 2], [2, 2, 1, 1], [1, 2]);
SELECT arrayIntersect([1, 1, 1], [1], [1], [1], [1]);
SELECT arrayIntersect([1, 2, 3], [3, 3, 3], [3, 2, 3]);

SELECT '-- the same for the other set modes';
SELECT arraySort(arraySymmetricDifference([1], [2], [1, 1]));
SELECT arraySort(arraySymmetricDifference([1], [2], [1]));
SELECT arraySort(arraySymmetricDifference([1, 1], [1], [1]));
SELECT arraySort(arrayUnion([1], [2], [1, 1]));
SELECT arraySort(arrayUnion([1, 1], [2, 2], [3, 3]));

SELECT '-- for the symmetric difference two arguments are enough to miscount a repeated value';
SELECT arraySort(arraySymmetricDifference([1], [2, 2]));
SELECT arraySort(arraySymmetricDifference([2, 2], [1]));
SELECT arraySort(arraySymmetricDifference([1, 1], [2, 2]));
SELECT arrayIntersect([1], [2, 2]);
SELECT arraySort(arrayUnion([1], [2, 2]));

SELECT '-- empty arguments';
SELECT arrayIntersect([1, 2, 3], []);
SELECT arrayIntersect([], [1, 2, 3]);
SELECT arrayIntersect([], [], []);
SELECT arraySort(arrayUnion([1, 2], []));
SELECT arraySort(arraySymmetricDifference([1, 2], []));

SELECT '-- the output keeps the order of the first argument';
SELECT arrayIntersect([5, 4, 3, 2, 1], [1, 2, 3]);
SELECT arrayIntersect([5, 4, 3, 2, 1], [3], [3, 2, 1]);

SELECT '-- nullable';
SELECT arraySort(arrayIntersect([1, NULL, 2], [NULL, 2, 3], [NULL, 2]));
SELECT arraySort(arrayIntersect([1, NULL, 2], [2, 3], [NULL, 2]));
SELECT arraySort(arrayIntersect([NULL, 2], [1, NULL, 2], [NULL, 2, 3]));
SELECT arraySort(arrayIntersect(materialize([1, NULL, 2]), [NULL, 2]));
SELECT arraySort(arraySymmetricDifference([1, NULL], [NULL], [NULL, 1]));

SELECT '-- strings';
SELECT arraySort(arrayIntersect(['a', 'b', 'c', 'd'], ['b'], ['b', 'c']));
SELECT arraySort(arrayIntersect(['b'], ['a', 'b', 'c', 'd'], ['b', 'c']));
SELECT arraySort(arrayIntersect(materialize(['a', 'b', 'c', 'd']), ['b', 'b'], ['b', 'c']));

SELECT '-- a large argument against a small one, both orders';
WITH range(100000) AS big, [7, 99999, 100000] AS small
SELECT arraySort(arrayIntersect(big, small)), arraySort(arrayIntersect(small, big));

SELECT '-- which argument is the smallest changes from row to row';
SELECT
    arraySort(arrayIntersect(a, b)) = arraySort(arrayIntersect(b, a)) AS same_both_ways,
    arraySort(arrayIntersect(a, b)) AS result
FROM
(
    SELECT range(number % 7) AS a, range(6 - (number % 7)) AS b FROM numbers(7)
);

SELECT '-- a single argument is returned deduplicated';
SELECT arrayIntersect([1, 1, 2, 2, 3]);
SELECT arraySort(arrayUnion([1, 1, 2, 2, 3]));

SELECT '-- the generic path, where the elements have to be serialized into an arena';
SELECT arrayIntersect([[1, 2], [3]], [[3], [4]]);
SELECT arraySort(arrayIntersect([(1, 'a'), (2, 'b')], [(2, 'b'), (3, 'c')], [(2, 'b'), (1, 'a')]));
SELECT arrayIntersect([[1]], [[2]], [[1], [1]]);
SELECT arraySort(arrayUnion([[1, 2]], [[2]], [[2], [3]]));
SELECT arraySymmetricDifference([[1]], [[2]], [[1], [1]]);
SELECT arraySort(arraySymmetricDifference([[1]], [[2], [2]]));
SELECT sum(length(arrayIntersect(a, b))) FROM (SELECT arrayMap(x -> [x, x + 1], range(number % 20)) AS a, arrayMap(x -> [x + 2, x + 3], range(number % 20)) AS b FROM numbers(10000));
