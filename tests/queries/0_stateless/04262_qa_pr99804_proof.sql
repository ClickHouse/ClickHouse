-- Tags: no-parallel, no-fasttest
-- Tag no-parallel: Messes with internal cache
-- Proof test for subquery caching missing overflow mode check
-- Bug: checkCanWriteQueryResultCache in Planner.cpp doesn't validate overflow modes,
-- allowing subqueries with overflow_mode != 'throw' to be cached

DROP TABLE IF EXISTS overflow_cache_test;

CREATE TABLE overflow_cache_test (x UInt64) ENGINE = MergeTree ORDER BY x AS SELECT number FROM numbers(100);

SYSTEM DROP QUERY CACHE;

-- Verify that the top-level query cache correctly rejects overflow modes (this works)
SELECT sum(x) FROM overflow_cache_test SETTINGS read_overflow_mode = 'break', use_query_cache = 1; -- { serverError QUERY_CACHE_USED_WITH_NON_THROW_OVERFLOW_MODE }

SYSTEM DROP QUERY CACHE;

-- Bug: Subquery caching path should also reject overflow modes but doesn't
-- Expected: throw QUERY_CACHE_USED_WITH_NON_THROW_OVERFLOW_MODE
-- Actual (buggy): Query succeeds and caches truncated results
SELECT * FROM (SELECT sum(x) FROM overflow_cache_test SETTINGS read_overflow_mode = 'break') SETTINGS use_query_cache = true, query_cache_for_subqueries = true; -- { serverError QUERY_CACHE_USED_WITH_NON_THROW_OVERFLOW_MODE }

SYSTEM DROP QUERY CACHE;

-- Test with group_by_overflow_mode (another overflow mode)
SELECT * FROM (SELECT sum(x) FROM overflow_cache_test SETTINGS group_by_overflow_mode = 'break') SETTINGS use_query_cache = true, query_cache_for_subqueries = true; -- { serverError QUERY_CACHE_USED_WITH_NON_THROW_OVERFLOW_MODE }

SYSTEM DROP QUERY CACHE;

DROP TABLE overflow_cache_test;
