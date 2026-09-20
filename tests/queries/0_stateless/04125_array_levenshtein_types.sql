-- Exercise arrayLevenshteinDistance / arrayLevenshteinDistanceWeighted /
-- arraySimilarity across the numeric / string / weight type fan-out in
-- Functions/array/arrayLevenshtein.cpp (levenshteinNumber, levenshteinString,
-- levenshteinWeightedNumber, levenshteinWeightedString, levenshteinWeightedGeneric).

SELECT '--- arrayLevenshteinDistance: numeric arrays ---';
SELECT arrayLevenshteinDistance([1, 2, 3]::Array(UInt8), [1, 2, 4]::Array(UInt8));
SELECT arrayLevenshteinDistance([1, 2, 3]::Array(Int16), [1, 2]::Array(Int16));
SELECT arrayLevenshteinDistance([1, 2, 3]::Array(UInt32), [4, 5, 6]::Array(UInt32));
SELECT arrayLevenshteinDistance([1, 2, 3]::Array(Int64), []::Array(Int64));
SELECT arrayLevenshteinDistance([1.5, 2.5]::Array(Float32), [1.5, 3.0]::Array(Float32));
SELECT arrayLevenshteinDistance([1.5, 2.5]::Array(Float64), [1.5, 3.0]::Array(Float64));

SELECT '--- arrayLevenshteinDistance: string arrays ---';
SELECT arrayLevenshteinDistance(['a', 'b', 'c'], ['a', 'b', 'd']);
SELECT arrayLevenshteinDistance(['foo'], ['bar', 'baz']);
SELECT arrayLevenshteinDistance([]::Array(String), ['x']);

SELECT '--- arrayLevenshteinDistanceWeighted: matching weight types ---';
SELECT arrayLevenshteinDistanceWeighted([1, 2, 3], [1, 2, 4], [1, 1, 1]::Array(UInt8), [1, 1, 1]::Array(UInt8));
SELECT arrayLevenshteinDistanceWeighted([1, 2, 3], [1, 2, 4], [1, 1, 1]::Array(UInt32), [1, 1, 1]::Array(UInt32));
SELECT arrayLevenshteinDistanceWeighted([1, 2, 3], [1, 2, 4], [1, 1, 1]::Array(Int64), [1, 1, 1]::Array(Int64));
SELECT arrayLevenshteinDistanceWeighted([1, 2, 3], [1, 2, 4], [1.0, 1.0, 1.0]::Array(Float32), [1.0, 1.0, 1.0]::Array(Float32));
SELECT arrayLevenshteinDistanceWeighted([1, 2, 3], [1, 2, 4], [1.0, 1.0, 1.0]::Array(Float64), [1.0, 1.0, 1.0]::Array(Float64));

SELECT '--- arrayLevenshteinDistanceWeighted: string values ---';
SELECT arrayLevenshteinDistanceWeighted(['a', 'b'], ['a', 'c'], [1, 1]::Array(UInt8), [1, 1]::Array(UInt8));

SELECT '--- arraySimilarity ---';
SELECT arraySimilarity(['a', 'b'], ['a', 'c'], [1, 1]::Array(UInt8), [1, 1]::Array(UInt8));
SELECT arraySimilarity([1, 2, 3], [1, 2, 4], [1, 1, 1]::Array(UInt32), [1, 1, 1]::Array(UInt32));
SELECT arraySimilarity([], []::Array(String), []::Array(UInt8), []::Array(UInt8));

SELECT '--- error: non-array first arg ---';
SELECT arrayLevenshteinDistance('not array', [1, 2]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '--- error: non-numeric weights ---';
SELECT arrayLevenshteinDistanceWeighted([1, 2], [1, 3], ['a', 'b'], ['a', 'c']); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '--- error: mismatched weight types ---';
SELECT arrayLevenshteinDistanceWeighted([1, 2], [1, 3], [1, 2]::Array(UInt8), [1, 2]::Array(UInt16)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '--- error: wrong number of arguments to arraySimilarity ---';
SELECT arraySimilarity([1, 2], [1, 3]); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT '--- arrayLevenshteinDistanceWeighted with const-not-always arrays ---';
SELECT arrayLevenshteinDistanceWeighted(
    materialize([1, 2, 3]::Array(UInt8)),
    materialize([1, 2, 4]::Array(UInt8)),
    [1, 2, 3]::Array(UInt8),
    [1, 2, 3]::Array(UInt8));

SELECT '--- run across multiple rows to hit the row-loop ---';
WITH CAST([1, 2, 3] AS Array(UInt32)) AS a, CAST([1, 2] AS Array(UInt32)) AS b
SELECT arrayLevenshteinDistance(a, b), arraySimilarity(a, b, [1, 1, 1]::Array(UInt32), [1, 1]::Array(UInt32))
FROM numbers(3);
