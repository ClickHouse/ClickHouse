-- STID 1941-1bfa: a `[i]` subscript on a `kql_array_sort_asc`/`kql_array_sort_desc`
-- result that is immediately followed by another equal-or-lower-priority operator
-- (`.j`, `[k]`, `+`, `=`, ...) aborted the server with `Inconsistent AST formatting`
-- LOGICAL_ERROR in debug / sanitizer builds.
--
-- `kql_array_sort_*` returns `Tuple(Array, ...)`, so `[i]` on its result is tuple
-- element access and is rewritten from `arrayElement` to `tupleElement` by the parser.
-- That rewrite lived only in `Layer::mergeElement`. When the subscript is followed by
-- another operator, the `arrayElement` node is instead built by the equal-priority
-- operator-merge loop in `ParserExpressionImpl::tryParseOperator`, which had no rewrite,
-- so a raw `arrayElement(kql_array_sort_*, i)` survived. The formatter emits that as
-- `[i]`, which re-parses (now without a trailing operator) through the rewrite as
-- `tupleElement`, so the format-parse-format round-trip diverged by one node.
--
-- The fix factors the rewrite into `Layer::foldKqlArraySortSubscript` and calls it from
-- both `arrayElement`-building sites.

-- Round-trip stability: the formatted output must be identical across format-parse-format.
-- Before the fix each of these aborted the server before producing any output.
SELECT formatQuerySingleLine('SELECT kql_array_sort_desc([3, 1, 2])[1]');
SELECT formatQuerySingleLine('SELECT kql_array_sort_asc([3, 1, 2])[1].2');
SELECT formatQuerySingleLine('SELECT kql_array_sort_desc([3, 1, 2])[1] + 1');
SELECT formatQuerySingleLine('SELECT kql_array_sort_asc([3, 1, 2])[1][1].1');
SELECT formatQuerySingleLine('SELECT kql_array_sort_desc([3, 1, 2])[1] = 1');

-- Call-syntax `arrayElement(kql_array_sort_*, i)` followed by an operator must round-trip
-- unchanged: only an operator-built subscript is tuple-element access, a user-written
-- `arrayElement(...)` call is preserved verbatim.
SELECT formatQuerySingleLine('SELECT arrayElement(kql_array_sort_asc([(1, 2)]), 1).1');
SELECT formatQuerySingleLine('SELECT arrayElement(kql_array_sort_desc([(1, 2)]), 1) = 1');

-- Force the round-trip check to run on the executable path. These are semantically
-- invalid (a tuple element index that does not exist / wrong type), so they report
-- ILLEGAL_TYPE_OF_ARGUMENT. The point is that the server reaches the type checker
-- instead of aborting with LOGICAL_ERROR.
SELECT kql_array_sort_desc([3, 1, 2])[1].2; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT kql_array_sort_asc([3, 1, 2])[1][1].1; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Valid evaluation semantics must still hold: single bracket returns the sorted array,
-- double bracket the scalar element, and a trailing operator applies to the scalar.
SELECT kql_array_sort_asc([3, 1, 2])[1];
SELECT kql_array_sort_asc([3, 1, 2])[1][2];
SELECT kql_array_sort_asc([3, 1, 2])[1][2] + 1;
