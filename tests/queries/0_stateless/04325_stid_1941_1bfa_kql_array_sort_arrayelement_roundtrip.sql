-- The bracket subscript on `kql_array_sort_asc` / `kql_array_sort_desc` was implemented by
-- an in-place rename of a sibling `arrayElement` operand to `tupleElement` inside the
-- operator-folding code. That rewrite also fired against an `arrayElement(kql_array_sort_*, ...)`
-- node built via call syntax, destroying the user-built shape and causing the format /
-- parse / format round-trip check to raise `Inconsistent AST formatting` (STID 1941-1bfa).
--
-- The fix only rewrites a sibling whose `is_operator` flag is set, so call-syntax
-- arrayElement nodes are preserved verbatim.

-- 1. Existing `kql_array_sort_*([...])[i]` syntax must still produce `tupleElement(kql, i)`.
SELECT formatQuery('SELECT kql_array_sort_asc([3, 1, 2])[1]');
SELECT formatQuery('SELECT kql_array_sort_desc([3, 1, 2])[1]');

-- 2. Existing `kql_array_sort_*([...])[i][j]` chained syntax must still produce
--    `arrayElement(tupleElement(kql, i), j)`.
SELECT formatQuery('SELECT kql_array_sort_asc([3, 1, 2])[1][2]');
SELECT formatQuery('SELECT kql_array_sort_desc([3, 1, 2])[1][2]');

-- 3. Call-syntax `arrayElement(kql_array_sort_*([...]), i)` must round-trip unchanged
--    (the inner `arrayElement` must NOT be silently rewritten to `tupleElement`).
SELECT formatQuery('SELECT arrayElement(kql_array_sort_asc([(1, 2)]), 1)');
SELECT formatQuery('SELECT arrayElement(kql_array_sort_desc([(1, 2)]), 1)');

-- 4. Call-syntax `arrayElement(arrayElement(kql_array_sort_*([...]), x), y)` must round-trip
--    unchanged. This was the BuzzHouse-failing shape.
SELECT formatQuery('SELECT arrayElement(arrayElement(kql_array_sort_asc([(1, 2)]), 1), 2)');
SELECT formatQuery('SELECT arrayElement(arrayElement(kql_array_sort_desc([(1, 2)]), 1), 2)');

-- 5. Mixed call-syntax inner + bracket outer: `arrayElement(kql_array_sort_*([...]), i)[j]`.
--    Pre-fix this triggered `Inconsistent AST formatting` because the outer bracket-fold
--    silently mutated the inner call-syntax `arrayElement` to `tupleElement`.
SELECT formatQuery('SELECT arrayElement(kql_array_sort_asc([(1, 2)]), 1)[2]');
SELECT formatQuery('SELECT arrayElement(kql_array_sort_desc([(1, 2)]), 1)[2]');

-- 6. KQL evaluation semantics: single bracket returns the array, double bracket the scalar.
SELECT kql_array_sort_asc([3, 1, 2])[1];
SELECT kql_array_sort_asc([3, 1, 2])[1][2];

-- 7. Force the format/parse/format round-trip check to run on the BuzzHouse-failing shape.
--    Pre-fix this raised LOGICAL_ERROR `Inconsistent AST formatting (STID: 1941-1bfa)`.
--    Post-fix the parser preserves the original shape and the type checker reports the
--    semantic error instead.
SELECT 1 WHERE (arrayElement(kql_array_sort_asc([(1, 2)]), 1)[2]).1 = 1; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT 1 WHERE (arrayElement(arrayElement(kql_array_sort_asc([(1, 2)]), 1), 2)).1 = 1; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
