-- Correctness guards for the identifier-resolution cache: enabling the cache must not
-- change results. Each scenario is run with the cache disabled and then enabled; the two
-- output blocks must be identical. These cover two context-sensitivity bugs found in review.

SET enable_analyzer = 1;

-- 1. Compound-alias shadowing. `value` is an alias for a tuple; a later `value.a` must bind
-- to that alias (the rewritten field), not to the source column `value.a` resolved while the
-- alias itself was being built. Caching keyed on the first identifier component prevents the
-- stale source-column resolution from being reused.
SELECT '-- 1: compound alias shadowing --';
SELECT CAST(tuple(value.a + 1), 'Tuple(a UInt64)') AS value, value.a AS out
FROM (SELECT CAST(tuple(10), 'Tuple(a UInt64)') AS value)
SETTINGS enable_identifier_resolve_cache = 0;
SELECT CAST(tuple(value.a + 1), 'Tuple(a UInt64)') AS value, value.a AS out
FROM (SELECT CAST(tuple(10), 'Tuple(a UInt64)') AS value)
SETTINGS enable_identifier_resolve_cache = 1;

-- 2. Function alias over a nullable group-by key with group_by_use_nulls. `a = k + 1` is
-- Nullable outside aggregates (HAVING) and non-nullable inside aggregate arguments (sum(a)).
-- A result whose subtree references the nullable key must not be cached and reused across
-- those contexts.
SELECT '-- 2: function alias over nullable group-by key --';
SELECT k + 1 AS a, sum(a) AS s
FROM (SELECT number AS k FROM numbers(3))
GROUP BY k WITH CUBE HAVING a IS NULL
ORDER BY a NULLS FIRST, s
SETTINGS group_by_use_nulls = 1, enable_identifier_resolve_cache = 0;
SELECT k + 1 AS a, sum(a) AS s
FROM (SELECT number AS k FROM numbers(3))
GROUP BY k WITH CUBE HAVING a IS NULL
ORDER BY a NULLS FIRST, s
SETTINGS group_by_use_nulls = 1, enable_identifier_resolve_cache = 1;
