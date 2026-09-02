-- Test that max_query_size = UINT64_MAX doesn't cause std::length_error
-- when an exception is thrown during query processing.
--
-- Bug description: In executeQuery.cpp, when an exception is thrown
-- and query.empty() is true, the code does:
--   query.assign(begin, std::min(end - begin, static_cast<ptrdiff_t>(max_query_size)))
-- When max_query_size is 18446744073709551615 (UINT64_MAX), casting it to ptrdiff_t
-- results in -1. Then std::min(positive, -1) returns -1, and query.assign(begin, -1)
-- converts -1 to SIZE_MAX, causing std::length_error.

-- The setting must be applied BEFORE the query is parsed (via SET, not inline SETTINGS)
-- to trigger the bug. This query uses an undefined query parameter, which triggers
-- an exception after parsing but before the query string is assigned.
-- Before the fix: std::length_error is thrown
-- After the fix: UNKNOWN_QUERY_PARAMETER error is returned properly

SET max_query_size = 18446744073709551615;
SELECT {undefined_param:UInt64}; -- {serverError UNKNOWN_QUERY_PARAMETER}
