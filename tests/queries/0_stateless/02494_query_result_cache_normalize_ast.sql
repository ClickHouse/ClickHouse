-- Tags: no-parallel

-- Warm up: If the query result cache has not been used yet, its event counters don't show up in
-- system.events (instead of simply being shown as 0). Insert into query result cache once to
-- initialize the hit (**) and miss counters (*).
SELECT 42 SETTINGS enable_experimental_query_result_cache = true; -- (*)
SELECT 42 SETTINGS enable_experimental_query_result_cache_passive_usage = true; -- (**)

SYSTEM DROP QUERY RESULT CACHE;
DROP TABLE IF EXISTS old;

-- save current event counts for query result cache
CREATE TABLE old (event String, value UInt64) ENGINE=MergeTree ORDER BY event;
INSERT INTO old SELECT event, value FROM system.events WHERE event LIKE 'QueryResultCache%';

-- Run query whose result gets cached in the query result cache (QRC).
-- Besides "enable_experimental_query_result_cache", pass two knobs (one QRC-specific knob and one non-QRC-specific knob). We just care
-- *that* they are passed and not about their effect.
SELECT 1 SETTINGS enable_experimental_query_result_cache = true, query_result_cache_store_results_of_queries_with_nondeterministic_functions = true, max_threads = 16;
SELECT COUNT(*) FROM system.query_result_cache;

SELECT '---';

-- Run same query again but with different SETTINGS. We want its result to be served from the QRC.
SELECT 1 SETTINGS enable_experimental_query_result_cache_passive_usage = true,  max_threads = 16;

-- Technically, both SELECT queries have different ASTs, leading to different QRC keys. Internal AST normalization makes sure that the keys
-- match regardless. Verify by checking that we had a cache hit.
SELECT value = (SELECT value FROM old WHERE event = 'QueryResultCacheHits') + 1
FROM system.events
WHERE event = 'QueryResultCacheHits';

SYSTEM DROP QUERY RESULT CACHE;
DROP TABLE old;
