-- Tags: no-parallel
-- ^ uses SYSTEM DROP QUERY CACHE, which would interfere with concurrent query cache tests.

-- The query-level `SETTINGS` clause of a nested `INTERSECT` / `EXCEPT` lives on the *last* operand
-- after `SelectIntersectExceptQueryVisitor` has normalized the query, while it still applies to every
-- operand. The query result cache's non-determinism walk must apply it before descending, otherwise an
-- `obfuscate(...)` in an earlier operand is inspected under the stale `obfuscate_seed` and the
-- determinism verdict comes out backwards in both directions.
--
-- `obfuscate` is an effectively infinite, repeating source, so every read of it needs an explicit `LIMIT`.

SET enable_analyzer = 1;

SYSTEM DROP QUERY CACHE TAG '05049_obfuscate_nested_intersect_except';

-- The session seed is deterministic, but the nested `EXCEPT` resets it to the default (empty) seed, so
-- the `obfuscate(...)` operand really runs with a fresh random seed and the query must be rejected for
-- caching.
SET obfuscate_seed = 'stable';
SELECT count() <= 4 FROM
(
    SELECT * FROM obfuscate(SELECT number FROM numbers(4)) LIMIT 4
    EXCEPT
    SELECT 1
    SETTINGS obfuscate_seed = DEFAULT
)
SETTINGS use_query_cache = 1; -- { serverError QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS }

-- The inverse: the session seed is the default (empty) one, but the nested `EXCEPT` pins a
-- deterministic seed for every operand, so the query is cacheable and must not be rejected.
SET obfuscate_seed = '';
SELECT count() <= 4 FROM
(
    SELECT * FROM obfuscate(SELECT number FROM numbers(4)) LIMIT 4
    EXCEPT
    SELECT 1
    SETTINGS obfuscate_seed = 'stable'
)
SETTINGS use_query_cache = 1, query_cache_tag = '05049_obfuscate_nested_intersect_except';

SYSTEM DROP QUERY CACHE TAG '05049_obfuscate_nested_intersect_except';
