-- Tags: no-parallel
-- Tag no-parallel: Messes with the internal query result cache

-- The `obfuscate` table function with an empty seed derives a fresh random seed for every execution,
-- so its result is non-deterministic and must not be written to the query result cache by default.
-- A non-empty `obfuscate_seed` makes the output reproducible and therefore cacheable.

SYSTEM CLEAR QUERY CACHE;

SELECT '-- empty seed is non-deterministic: rejected by the query cache by default';
SELECT count() FROM (SELECT * FROM obfuscate(SELECT * FROM numbers(8)) LIMIT 8) SETTINGS use_query_cache = 1; -- { serverError QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS }
SELECT count() FROM system.query_cache;

SYSTEM CLEAR QUERY CACHE;

SELECT '-- non-empty seed is deterministic: cacheable';
SELECT count() FROM (SELECT * FROM obfuscate(SELECT * FROM numbers(8)) LIMIT 8) SETTINGS use_query_cache = 1, obfuscate_seed = 'stable';
SELECT count() FROM system.query_cache;

SYSTEM CLEAR QUERY CACHE;

SELECT '-- empty seed in an inner SETTINGS overrides an outer non-empty seed: rejected';
SELECT count() FROM (SELECT * FROM obfuscate(SELECT * FROM numbers(8)) LIMIT 8 SETTINGS obfuscate_seed = '') SETTINGS use_query_cache = 1, obfuscate_seed = 'stable'; -- { serverError QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS }
SELECT count() FROM system.query_cache;

SYSTEM CLEAR QUERY CACHE;

SELECT '-- non-empty seed in an inner SETTINGS makes the result reproducible: cacheable';
SELECT count() FROM (SELECT * FROM obfuscate(SELECT * FROM numbers(8)) LIMIT 8 SETTINGS obfuscate_seed = 'stable') SETTINGS use_query_cache = 1;
SELECT count() FROM system.query_cache;

SYSTEM CLEAR QUERY CACHE;
