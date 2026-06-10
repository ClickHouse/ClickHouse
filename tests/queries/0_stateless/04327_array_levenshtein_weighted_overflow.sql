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

SELECT '--- large integer weights stay exact (sum accumulated without rounding) ---';
-- Empty-side shortcut: 9007199254740992 + 1 + 1 must accumulate exactly to 9007199254740994
-- (representable in Float64), not collapse to 9007199254740992 as plain Float64 adds would.
SELECT toUInt64(arrayLevenshteinDistanceWeighted([]::Array(UInt8), [1, 2, 3]::Array(UInt8), []::Array(Int64), [9007199254740992, 1, 1]::Array(Int64)));
-- Same exactness on the DP path: cost accumulates 9007199254740992 + 1 + 1 = 9007199254740994,
-- which plain Float64 adds would round back down to 9007199254740992.
SELECT toUInt64(arrayLevenshteinDistanceWeighted(['a'], ['b', 'c'], [9007199254740992]::Array(Int64), [1, 1]::Array(Int64)));
-- Unsigned wide accumulator stays exact too: 2^53 + 2 over two UInt64 weights.
SELECT toUInt64(arrayLevenshteinDistanceWeighted([]::Array(UInt8), [1, 2, 3]::Array(UInt8), []::Array(UInt64), [9007199254740992, 1, 1]::Array(UInt64)));
