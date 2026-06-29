-- Test arraySimilarity when total weight sum is zero but distance is non-zero (mixed-sign weights),
-- exercising the weights_sum==0 guard against division by zero.
SELECT '--- distance is non-zero while weights sum to zero ---';
SELECT arrayLevenshteinDistanceWeighted(['a', 'a'], ['a'], [-100, 1]::Array(Int64), [99]::Array(Int64));
SELECT arrayLevenshteinDistanceWeighted(['a', 'a', 'a'], ['a'], [-50, -50, 1]::Array(Int64), [99]::Array(Int64));
SELECT arrayLevenshteinDistanceWeighted(['a', 'a'], ['a'], [-8, 1]::Array(Int32), [7]::Array(Int32));
SELECT '--- arraySimilarity returns 1.0 via the weights_sum==0 branch (no division by zero) ---';
SELECT arraySimilarity(['a', 'a'], ['a'], [-100, 1]::Array(Int64), [99]::Array(Int64));
SELECT arraySimilarity(['a', 'a', 'a'], ['a'], [-50, -50, 1]::Array(Int64), [99]::Array(Int64));
SELECT arraySimilarity(['a', 'a'], ['a'], [-8, 1]::Array(Int32), [7]::Array(Int32));
SELECT '--- result stays finite ---';
SELECT isFinite(arraySimilarity(['a', 'a'], ['a'], [-100, 1]::Array(Int64), [99]::Array(Int64)));
