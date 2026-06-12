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

SELECT '--- widest (256-bit) weights do not wrap the accumulator ---';
-- UInt256::max() + 1 + 1 must stay a large positive distance, not wrap the accumulator to 0.
SELECT arrayLevenshteinDistanceWeighted([]::Array(UInt8), [1, 2, 3]::Array(UInt8), []::Array(UInt256), [toUInt256('115792089237316195423570985008687907853269984665640564039457584007913129639935'), 1, 1]::Array(UInt256)) > 0;
-- arraySimilarity must not report perfect similarity (1) for a total mismatch with such weights.
SELECT arraySimilarity([]::Array(UInt8), [1, 2, 3]::Array(UInt8), []::Array(UInt256), [toUInt256('115792089237316195423570985008687907853269984665640564039457584007913129639935'), 1, 1]::Array(UInt256));
-- The same large UInt256 weight on the DP (insertion) path stays positive.
SELECT arrayLevenshteinDistanceWeighted(['a'], ['b', 'c'], [toUInt256('115792089237316195423570985008687907853269984665640564039457584007913129639935')]::Array(UInt256), [1, 1]::Array(UInt256)) > 0;
-- Signed Int256::max() summed twice must stay positive (no signed wrap to a negative value).
SELECT arrayLevenshteinDistanceWeighted([]::Array(UInt8), [1, 2]::Array(UInt8), []::Array(Int256), [toInt256('57896044618658097711785492504343953926634992332820282019728792003956564819967'), toInt256('57896044618658097711785492504343953926634992332820282019728792003956564819967')]::Array(Int256)) > 0;
SELECT isFinite(arrayLevenshteinDistanceWeighted([]::Array(UInt8), [1, 2, 3]::Array(UInt8), []::Array(UInt256), [toUInt256('115792089237316195423570985008687907853269984665640564039457584007913129639935'), 1, 1]::Array(UInt256)));
