SELECT 'ArrayFirstIndex constant predicate';
SELECT arrayFirstIndex(x -> 1, emptyArrayUInt8());
SELECT arrayFirstIndex(x -> 0, emptyArrayUInt8());
SELECT arrayFirstIndex(x -> 1, [1, 2, 3]);
SELECT arrayFirstIndex(x -> 0, [1, 2, 3]);

SELECT 'ArrayFirstIndex non constant predicate';
SELECT arrayFirstIndex(x -> x >= 2, emptyArrayUInt8());
SELECT arrayFirstIndex(x -> x >= 2, [1, 2, 3]);
SELECT arrayFirstIndex(x -> x >= 2, [1, 2, 3]);

SELECT 'ArrayLastIndex constant predicate';
SELECT arrayLastIndex(x -> 1, emptyArrayUInt8());
SELECT arrayLastIndex(x -> 0, emptyArrayUInt8());
SELECT arrayLastIndex(x -> 1, [1, 2, 3]);
SELECT arrayLastIndex(x -> 0, materialize([1, 2, 3]));

SELECT 'ArrayLastIndex non constant predicate';
SELECT arrayLastIndex(x -> x >= 2, emptyArrayUInt8());
SELECT arrayLastIndex(x -> x >= 2, [1, 2, 3]);
SELECT arrayLastIndex(x -> x >= 2, materialize([1, 2, 3]));
