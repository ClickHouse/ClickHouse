-- Test that arrayFilter correctly handles lambdas with nullable return types.
-- This tests the fix for a bug where the function cache was keyed by tree hash
-- computed before lambda resolution, causing type mismatches in debug builds.

-- These two subqueries have arrayFilter with IDENTICAL AST structure:
--   arrayFilter(x -> (x = a), [1])
-- But 'a' resolves to different types (UInt8 vs Nullable(UInt8)).
-- Before the fix, the function cache would incorrectly reuse the first
-- function_base for the second call, causing a type mismatch.
SELECT
    (SELECT arrayFilter(x -> (x = a), [1]) FROM (SELECT 1 AS a)),
    (SELECT arrayFilter(x -> (x = a), [1]) FROM (SELECT CAST(NULL AS Nullable(UInt8)) AS a));

-- Similar test with the nullable one first
SELECT
    (SELECT arrayFilter(x -> (x = a), [1]) FROM (SELECT CAST(NULL AS Nullable(UInt8)) AS a)),
    (SELECT arrayFilter(x -> (x = a), [1]) FROM (SELECT 1 AS a));

-- Test with explicit _CAST to nullable (from fuzzer)
SELECT arrayFilter(x -> (_CAST(1, 'Nullable(UInt8)') = x), _CAST([1], 'Array(UInt8)'));

-- Simple cases
SELECT arrayFilter(x -> (_CAST(1, 'Nullable(UInt8)') = x), [1, 2, 3]);
SELECT arrayFilter(x -> (x = _CAST(1, 'Nullable(UInt8)')), [1, 2, 3]);

-- Test with NULL literal
SELECT arrayFilter(x -> (NULL = x), [1, 2, 3]);
SELECT arrayFilter(x -> (x = NULL), [1, 2, 3]);

-- Test with nullable column from subquery
SELECT arrayFilter(x -> (x = n), [1, 2, 3]) FROM (SELECT CAST(1 AS Nullable(UInt8)) AS n);

-- Verify return types are handled correctly
SELECT toTypeName(arrayFilter(x -> (x = 1), [1, 2, 3]));
SELECT toTypeName(arrayFilter(x -> (x = _CAST(1, 'Nullable(UInt8)')), [1, 2, 3]));
