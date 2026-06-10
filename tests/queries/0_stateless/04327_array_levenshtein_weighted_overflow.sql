SELECT '--- large integer weights do not overflow ---';
SELECT arrayLevenshteinDistanceWeighted(['a', 'b'], ['c', 'd'], [9223372036854775807, 9223372036854775807]::Array(Int64), [9223372036854775807, 9223372036854775807]::Array(Int64));
SELECT arrayLevenshteinDistanceWeighted(['a', 'b', 'c'], ['x'], [9223372036854775807, 9223372036854775807, 1]::Array(Int64), [5]::Array(Int64));
SELECT arrayLevenshteinDistanceWeighted([]::Array(String), ['a', 'b'], []::Array(Int64), [9223372036854775807, 9223372036854775807]::Array(Int64));
SELECT arrayLevenshteinDistanceWeighted([1, 2], [3, 4], [18446744073709551615, 18446744073709551615]::Array(UInt64), [18446744073709551615, 18446744073709551615]::Array(UInt64));

SELECT '--- arraySimilarity weights_sum does not overflow ---';
SELECT arraySimilarity(['a', 'b'], ['c', 'd'], [9223372036854775807, 9223372036854775807]::Array(Int64), [9223372036854775807, 9223372036854775807]::Array(Int64));

SELECT '--- results stay finite ---';
SELECT isFinite(arrayLevenshteinDistanceWeighted(['a', 'b'], ['c', 'd'], [9223372036854775807, 9223372036854775807]::Array(Int64), [9223372036854775807, 9223372036854775807]::Array(Int64)));
SELECT isFinite(arraySimilarity(['a', 'b'], ['c', 'd'], [9223372036854775807, 9223372036854775807]::Array(Int64), [9223372036854775807, 9223372036854775807]::Array(Int64)));
