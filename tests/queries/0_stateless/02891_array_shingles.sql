SELECT '-- negative tests';
SELECT arrayShingles(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT arrayShingles([1, 2, 3, 4, 5]); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT arrayShingles([1, 2, 3, 4, 5], 2, 3); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT arrayShingles([1, 2, 3, 4, 5], 'str'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayShingles((1, 2, 3, 4, 5), 0); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayShingles([1, 2, 3, 4, 5], 0); -- { serverError BAD_ARGUMENTS }
SELECT arrayShingles([1, 2, 3, 4, 5], -2); -- { serverError BAD_ARGUMENTS }
SELECT arrayShingles([1, 2, 3, 4, 5], 6); -- { serverError BAD_ARGUMENTS }
SELECT arrayShingles([], 1); -- { serverError BAD_ARGUMENTS }

SELECT '-- const and non-const inputs';
SELECT [1, 2, 3, 4, 5] AS arr, 1 AS len, arrayShingles(arr, len), arrayShingles(materialize(arr), len);
SELECT [1, 2, 3, 4, 5] AS arr, 3 AS len, arrayShingles(arr, len), arrayShingles(materialize(arr), len);
SELECT [1, 2 ,3, 4, 5] AS arr, 5 AS len, arrayShingles(arr, len), arrayShingles(materialize(arr), len);

SELECT ['ab', 'c', 'de', '', 'hi'] AS arr, 1 AS len, arrayShingles(arr, len), arrayShingles(materialize(arr), len);
SELECT ['ab', 'c', 'de', '', 'hi'] AS arr, 3 AS len, arrayShingles(arr, len), arrayShingles(materialize(arr), len);
SELECT ['ab', 'c', 'de', '', 'hi'] AS arr, 5 AS len, arrayShingles(arr, len), arrayShingles(materialize(arr), len);

SELECT '-- special cases';
SELECT arrayShingles([toNullable(2), toNullable(1)], 1);
SELECT arrayShingles([toLowCardinality(2), toLowCardinality(1)], 1);
