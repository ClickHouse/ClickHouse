-- Tags: no-parallel
-- ^ uses SYSTEM DROP QUERY CACHE, which would interfere with concurrent query cache tests.

-- A query-level `SETTINGS` clause governs the whole (sub)query it belongs to, including the parts of
-- the AST that syntactically precede it. For the *top-level* query `executeQuery` has already applied
-- that clause to the context by the time the query result cache checks the query for non-deterministic
-- functions, but for a *nested* one it has not: the effective `obfuscate_seed` must be recomputed
-- while descending into the AST.
--
-- The clause of a nested `UNION` is the interesting case, because the parser leaves it on the *last*
-- arm's `ASTSelectQuery` rather than on `ASTQueryWithOutput::settings_ast`, while it still applies to
-- every arm. A naive left-to-right walk therefore sees an `obfuscate(...)` in an earlier arm under the
-- stale seed and gets the determinism verdict backwards in both directions.
--
-- `obfuscate` is an effectively infinite, repeating source, so every read of it needs an explicit `LIMIT`.

SET allow_experimental_analyzer = 1;

-- A unique `query_cache_tag` keeps the entry isolated from other tests sharing the server-global cache.
SYSTEM DROP QUERY CACHE TAG '05024_obfuscate_nested_union';

-- The session seed is deterministic, but the nested union resets it to the default (empty) seed, so the
-- `obfuscate(...)` arm really runs with a fresh random seed and the query must be rejected for caching.
SET obfuscate_seed = 'stable';
SELECT count() FROM
(
    SELECT * FROM obfuscate(SELECT number FROM numbers(4)) LIMIT 4
    UNION ALL
    SELECT 1
    SETTINGS obfuscate_seed = DEFAULT
)
SETTINGS use_query_cache = 1; -- { serverError QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS }

-- The inverse: the session seed is the default (empty) one, but the nested union pins a deterministic
-- seed for every arm, so the query is cacheable and must not be rejected.
SET obfuscate_seed = '';
SELECT count() FROM
(
    SELECT * FROM obfuscate(SELECT number FROM numbers(4)) LIMIT 4
    UNION ALL
    SELECT 1
    SETTINGS obfuscate_seed = 'stable'
)
SETTINGS use_query_cache = 1, query_cache_tag = '05024_obfuscate_nested_union';

SYSTEM DROP QUERY CACHE TAG '05024_obfuscate_nested_union';
