-- Tags: no-parallel
-- no-parallel: settings profiles are server-global and their names cannot be made unique per run,
-- and the test uses SYSTEM DROP QUERY CACHE TAG.

-- A nested `SETTINGS profile = '...'` can change the effective `obfuscate_seed` of a subquery without
-- mentioning `obfuscate_seed` itself. The query result cache must judge the determinism of
-- `obfuscate(...)` by the seed the subquery really runs with.

SET allow_experimental_analyzer = 1;

DROP SETTINGS PROFILE IF EXISTS profile_05332_empty_seed;
DROP SETTINGS PROFILE IF EXISTS profile_05332_stable_seed;
CREATE SETTINGS PROFILE profile_05332_empty_seed SETTINGS obfuscate_seed = '';
CREATE SETTINGS PROFILE profile_05332_stable_seed SETTINGS obfuscate_seed = 'stable';

SYSTEM DROP QUERY CACHE TAG '05332_obfuscate_profile_seed';

-- The session seed is deterministic, but the subquery switches to a profile with the empty seed,
-- so `obfuscate(...)` runs with a fresh random seed and must not be cached.
SET obfuscate_seed = 'stable';
SELECT count() FROM
(
    SELECT * FROM obfuscate(SELECT number FROM numbers(4)) LIMIT 4
    SETTINGS profile = 'profile_05332_empty_seed'
)
SETTINGS use_query_cache = 1; -- { serverError QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS }

-- The same with the per-subquery opt-in to the cache.
SELECT count() FROM
(
    SELECT * FROM obfuscate(SELECT number FROM numbers(4)) LIMIT 4
    SETTINGS profile = 'profile_05332_empty_seed', use_query_cache = 1
); -- { serverError QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS }

-- The order inside the clause matters: the profile applied last wins over an explicit seed.
SELECT count() FROM
(
    SELECT * FROM obfuscate(SELECT number FROM numbers(4)) LIMIT 4
    SETTINGS obfuscate_seed = 'stable', profile = 'profile_05332_empty_seed'
)
SETTINGS use_query_cache = 1; -- { serverError QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS }

-- The inverse: the session seed is empty, but the subquery switches to a profile with a deterministic
-- seed, so the query is cacheable.
SET obfuscate_seed = '';
SELECT count() FROM
(
    SELECT * FROM obfuscate(SELECT number FROM numbers(4)) LIMIT 4
    SETTINGS profile = 'profile_05332_stable_seed'
)
SETTINGS use_query_cache = 1, query_cache_tag = '05332_obfuscate_profile_seed';

SELECT count() FROM
(
    SELECT * FROM obfuscate(SELECT number FROM numbers(4)) LIMIT 4
    SETTINGS profile = 'profile_05332_stable_seed', use_query_cache = 1, query_cache_tag = '05332_obfuscate_profile_seed'
);

SYSTEM DROP QUERY CACHE TAG '05332_obfuscate_profile_seed';
DROP SETTINGS PROFILE profile_05332_empty_seed;
DROP SETTINGS PROFILE profile_05332_stable_seed;
