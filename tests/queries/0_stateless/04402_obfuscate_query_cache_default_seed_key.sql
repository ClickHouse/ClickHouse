-- Regression test for a query result cache key collision around `SETTINGS obfuscate_seed = DEFAULT`.
--
-- `SETTINGS obfuscate_seed = DEFAULT` is parsed into `ASTSetQuery::default_settings`, not into
-- `changes`. Two bugs combined to let it slip out of the cache key:
--   * `removeQueryResultCacheSettings` dropped the whole `SETTINGS` clause once `changes` was empty,
--     ignoring `default_settings`;
--   * `ASTSetQuery::updateTreeHashImpl` hashed only `changes`, not `default_settings`.
-- As a result a query with an inner `SETTINGS obfuscate_seed = DEFAULT` (effective seed reset to the
-- empty, non-deterministic value) hashed identically to the same query without the reset (effective
-- seed taken from the session). With `query_cache_nondeterministic_function_handling = 'save'` the
-- random-seeded query could write an entry that the deterministic session-seeded query then read.
--
-- A unique `query_cache_tag` isolates the two entries from any other test sharing the server-global
-- query cache, so the test does not need `no-parallel`.

SET allow_experimental_analyzer = 1;
SET obfuscate_seed = 'stable';

SYSTEM DROP QUERY CACHE TAG '04402_obfuscate';

-- Non-deterministic variant: the inner scope resets the seed to the default (empty) value, so the
-- obfuscator uses a fresh random seed. Caching is allowed only because of the 'save' handling.
SELECT count() FROM (SELECT * FROM obfuscate(SELECT number FROM numbers(4)) SETTINGS obfuscate_seed = DEFAULT)
SETTINGS use_query_cache = 1, query_cache_nondeterministic_function_handling = 'save', query_cache_min_query_runs = 0, query_cache_tag = '04402_obfuscate'
FORMAT Null;

-- Deterministic variant: the inner scope keeps the session seed 'stable'.
SELECT count() FROM (SELECT * FROM obfuscate(SELECT number FROM numbers(4)))
SETTINGS use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_tag = '04402_obfuscate'
FORMAT Null;

-- The two queries must occupy two distinct cache entries. Before the fix they collided into one and
-- the second (deterministic) query read the first (random) query's result.
SELECT count() FROM system.query_cache WHERE tag = '04402_obfuscate';

SYSTEM DROP QUERY CACHE TAG '04402_obfuscate';
