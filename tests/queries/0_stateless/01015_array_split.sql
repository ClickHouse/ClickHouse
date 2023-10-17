SELECT arraySplit((x, y) -> y, [1, 2, 3, 4, 5], [1, 0, 0, 1, 0]);
SELECT arrayReverseSplit((x, y) -> y, [1, 2, 3, 4, 5], [1, 0, 0, 1, 0]);

SELECT arraySplit(x -> 0, [1, 2, 3, 4, 5]);
SELECT arrayReverseSplit(x -> 0, [1, 2, 3, 4, 5]);
SELECT arraySplit(x -> 1, [1, 2, 3, 4, 5]);
SELECT arrayReverseSplit(x -> 1, [1, 2, 3, 4, 5]);
SELECT arraySplit(x -> x % 2 = 1, [1, 2, 3, 4, 5]);
SELECT arrayReverseSplit(x -> x % 2 = 1, [1, 2, 3, 4, 5]);

SELECT arraySplit(x -> 0, []);
SELECT arrayReverseSplit(x -> 0, []);
SELECT arraySplit(x -> 1, []);
SELECT arrayReverseSplit(x -> 1, []);
SELECT arraySplit(x -> x, emptyArrayUInt8());
SELECT arrayReverseSplit(x -> x, emptyArrayUInt8());

SELECT arraySplit(x -> x % 2 = 1, [1]);
SELECT arrayReverseSplit(x -> x % 2 = 1, [1]);
SELECT arraySplit(x -> x % 2 = 1, [2]);
SELECT arrayReverseSplit(x -> x % 2 = 1, [2]);
