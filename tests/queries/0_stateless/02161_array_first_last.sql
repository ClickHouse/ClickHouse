SELECT 'ArrayFirst constant predicate';
SELECT arrayFirst(x -> 1, emptyArrayUInt8());
SELECT arrayFirst(x -> 0, emptyArrayUInt8());
SELECT arrayFirst(x -> 1, [1, 2, 3]);
SELECT arrayFirst(x -> 0, [1, 2, 3]);

SELECT 'ArrayFirst non constant predicate';
SELECT arrayFirst(x -> x >= 2, emptyArrayUInt8());
SELECT arrayFirst(x -> x >= 2, [1, 2, 3]);
SELECT arrayFirst(x -> x >= 2, materialize([1, 2, 3]));

SELECT 'ArrayLast constant predicate';
SELECT arrayLast(x -> 1, emptyArrayUInt8());
SELECT arrayLast(x -> 0, emptyArrayUInt8());
SELECT arrayLast(x -> 1, [1, 2, 3]);
SELECT arrayLast(x -> 0, [1, 2, 3]);

SELECT 'ArrayLast non constant predicate';
SELECT arrayLast(x -> x >= 2, emptyArrayUInt8());
SELECT arrayLast(x -> x >= 2, [1, 2, 3]);
SELECT arrayLast(x -> x >= 2, materialize([1, 2, 3]));
