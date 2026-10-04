-- Tags: no-parallel
-- Tag no-parallel: issues `SYSTEM DROP QUERY CACHE`, which the style check requires to run serially.

-- Regression test pinning the query result cache key separation for `SETTINGS obfuscate_seed = DEFAULT`
-- in the two outer-level clause positions that `04402_obfuscate_query_cache_default_seed_key` does not
-- cover (that test covers the inner-subquery reset):
--
--   * a trailing `SETTINGS` after `SELECT ... UNION ALL SELECT ...` — parsed onto the last
--     `ASTSelectQuery::settings()` (`ASTSetQuery::default_settings`), not onto
--     `ASTQueryWithOutput::settings_ast`;
--   * `SETTINGS` after `FORMAT` — the only shape (besides a second trailing clause) that lands in
--     `ASTQueryWithOutput::settings_ast`.
--
-- In both shapes the outer-level reset changes the effective `obfuscate_seed` in the execution context,
-- and `obfuscate_seed` is not in `isSettingIgnoredInQueryResultCache`, so the effective changed settings
-- hashed into the cache key already separate the random-seeded variant from the deterministic
-- session-seeded one. The random-seeded variant must never share a cache entry that the deterministic
-- variant then reads, and vice versa, even with `query_cache_nondeterministic_function_handling = 'save'`.
--
-- Unique `query_cache_tag` values isolate the entries from other tests sharing the server-global query
-- cache; the cleanup `SYSTEM DROP QUERY CACHE TAG` keeps the test repeatable across re-runs.

SET allow_experimental_analyzer = 1;
SET obfuscate_seed = 'stable';

SYSTEM DROP QUERY CACHE TAG '04624_obfuscate_union';
SYSTEM DROP QUERY CACHE TAG '04624_obfuscate_output';

-- Shape 1: trailing `SETTINGS` after a UNION. Non-deterministic variant: the trailing clause resets the
-- seed to the default (empty) value, so the obfuscator uses a fresh random seed and caching is allowed
-- only because of the 'save' handling. `obfuscate(...)` is an effectively infinite, repeating source, so
-- the inner subqueries need an explicit `LIMIT`.
SELECT count() FROM (SELECT * FROM obfuscate(SELECT number FROM numbers(4)) LIMIT 4)
UNION ALL
SELECT count() FROM (SELECT * FROM obfuscate(SELECT number FROM numbers(4)) LIMIT 4)
SETTINGS obfuscate_seed = DEFAULT, use_query_cache = 1, query_cache_nondeterministic_function_handling = 'save', query_cache_min_query_runs = 0, query_cache_tag = '04624_obfuscate_union'
FORMAT Null;

-- Deterministic variant: keeps the session seed 'stable'.
SELECT count() FROM (SELECT * FROM obfuscate(SELECT number FROM numbers(4)) LIMIT 4)
UNION ALL
SELECT count() FROM (SELECT * FROM obfuscate(SELECT number FROM numbers(4)) LIMIT 4)
SETTINGS use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_tag = '04624_obfuscate_union'
FORMAT Null;

-- The two queries must occupy two distinct cache entries.
SELECT count() FROM system.query_cache WHERE tag = '04624_obfuscate_union';

-- Shape 2: `SETTINGS` after `FORMAT`, which is parsed into `ASTQueryWithOutput::settings_ast`.
SELECT count() FROM (SELECT * FROM obfuscate(SELECT number FROM numbers(4)) LIMIT 4)
FORMAT Null
SETTINGS obfuscate_seed = DEFAULT, use_query_cache = 1, query_cache_nondeterministic_function_handling = 'save', query_cache_min_query_runs = 0, query_cache_tag = '04624_obfuscate_output';

SELECT count() FROM (SELECT * FROM obfuscate(SELECT number FROM numbers(4)) LIMIT 4)
FORMAT Null
SETTINGS use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_tag = '04624_obfuscate_output';

-- The two queries must occupy two distinct cache entries.
SELECT count() FROM system.query_cache WHERE tag = '04624_obfuscate_output';

SYSTEM DROP QUERY CACHE TAG '04624_obfuscate_union';
SYSTEM DROP QUERY CACHE TAG '04624_obfuscate_output';
