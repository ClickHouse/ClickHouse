-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

-- Caching the result of a query whose value depends on the rows processed before it would serve
-- a stale answer, so the query result cache refuses `runningConcurrency` at the default
-- `query_cache_nondeterministic_function_handling`, with `'save'` as the escape hatch. This lives
-- apart from 04872 because the cache is server-global and 04872 is parallel-safe.

SYSTEM DROP QUERY CACHE;

DROP TABLE IF EXISTS events_04874;
CREATE TABLE events_04874 (a UInt64, s DateTime, e DateTime) ENGINE = Memory;
INSERT INTO events_04874 VALUES (1, '2020-01-01 00:00:00', '2020-01-01 00:00:10');

SELECT runningConcurrency(s, e) FROM events_04874
    SETTINGS use_query_cache = 1; -- { serverError QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS }

SELECT 'save', runningConcurrency(s, e) FROM events_04874
    SETTINGS use_query_cache = 1, query_cache_nondeterministic_function_handling = 'save';

-- Sibling control: a running function that was already non-deterministic is refused the same way.
SELECT rowNumberInAllBlocks() FROM events_04874
    SETTINGS use_query_cache = 1; -- { serverError QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS }

-- Negative control: a deterministic query still caches.
SELECT 'deterministic', a FROM events_04874 SETTINGS use_query_cache = 1;

SYSTEM DROP QUERY CACHE;
DROP TABLE events_04874;
