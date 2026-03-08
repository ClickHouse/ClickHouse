-- Tags: no-fasttest
-- Verify that CAST to Nullable(FixedString(N)) returns NULL instead of throwing
-- when the source string is too long to fit in FixedString(N).
-- See https://github.com/ClickHouse/ClickHouse/issues/77464

-- Test 1: CAST to Nullable(FixedString(N)) with oversized string should return NULL
SELECT CAST('abc', 'Nullable(FixedString(2))');

-- Test 2: accurateCastOrNull should also return NULL (already works)
SELECT accurateCastOrNull('abc', 'FixedString(2)');

-- Test 3: CAST to Nullable(FixedString(N)) with matching size should work
SELECT CAST('ab', 'Nullable(FixedString(2))');

-- Test 4: CAST to non-Nullable FixedString with oversized string should still throw
SELECT CAST('abc', 'FixedString(2)'); -- { serverError TOO_LARGE_STRING_SIZE }

-- Test 5: nullIf + CAST to Nullable(FixedString(N)) — the original bug report
SELECT CAST(nullIf('abc', 'abc'), 'Nullable(FixedString(2))');

-- Test 6: CAST to Nullable(FixedString(N)) with shorter string should work
SELECT CAST('a', 'Nullable(FixedString(2))');
