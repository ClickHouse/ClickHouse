-- Tags: no-parallel-replicas

-- Regression test for issue #109044.
-- A cyclic alias whose name collides with a source column and is preceded by a bare
-- reference to that column used to resolve differently depending on
-- enable_identifier_resolve_cache: with the cache off it was rejected, with the cache
-- on (the default) it returned an internally inconsistent result. The resolution must be
-- the same for both cache states, and a cyclic alias must be rejected as CYCLIC_ALIASES.

SET enable_analyzer = 1;

-- The reported query: alias val = val + prev, alias prev = val + 1 (val -> prev -> val).
SELECT val, val + 1 AS prev, val + prev AS val FROM (SELECT 1 AS val)
SETTINGS enable_identifier_resolve_cache = 0; -- { serverError CYCLIC_ALIASES }
SELECT val, val + 1 AS prev, val + prev AS val FROM (SELECT 1 AS val)
SETTINGS enable_identifier_resolve_cache = 1; -- { serverError CYCLIC_ALIASES }

-- Same cycle without the leading bare reference: already rejected before, must stay rejected.
SELECT val + 1 AS prev, val + prev AS val FROM (SELECT 1 AS val)
SETTINGS enable_identifier_resolve_cache = 0; -- { serverError CYCLIC_ALIASES }
SELECT val + 1 AS prev, val + prev AS val FROM (SELECT 1 AS val)
SETTINGS enable_identifier_resolve_cache = 1; -- { serverError CYCLIC_ALIASES }

-- A cycle whose alias names do not collide with any source column.
SELECT a + 1 AS b, b + 1 AS a FROM (SELECT 1 AS x)
SETTINGS enable_identifier_resolve_cache = 0; -- { serverError CYCLIC_ALIASES }
SELECT a + 1 AS b, b + 1 AS a FROM (SELECT 1 AS x)
SETTINGS enable_identifier_resolve_cache = 1; -- { serverError CYCLIC_ALIASES }

-- Non-cyclic patterns must keep working and stay identical on both cache states.

-- Alias reused later in the projection list (the pattern the resolve cache optimizes).
SELECT val, val + 1 AS prev, val + prev AS val2 FROM (SELECT 1 AS val)
SETTINGS enable_identifier_resolve_cache = 0;
SELECT val, val + 1 AS prev, val + prev AS val2 FROM (SELECT 1 AS val)
SETTINGS enable_identifier_resolve_cache = 1;

-- Alias that shadows a source column but is not cyclic.
SELECT id, id + 1 AS id FROM (SELECT 1 AS id)
SETTINGS enable_identifier_resolve_cache = 0;
SELECT id, id + 1 AS id FROM (SELECT 1 AS id)
SETTINGS enable_identifier_resolve_cache = 1;

-- Transitive alias that is not a cycle.
WITH path('clickhouse.com/a/b/c') AS x SELECT x AS path
SETTINGS enable_identifier_resolve_cache = 0;
WITH path('clickhouse.com/a/b/c') AS x SELECT x AS path
SETTINGS enable_identifier_resolve_cache = 1;

-- A plain unknown identifier is still UNKNOWN_IDENTIFIER, not CYCLIC_ALIASES.
SELECT missing_column FROM (SELECT 1 AS val)
SETTINGS enable_identifier_resolve_cache = 0; -- { serverError UNKNOWN_IDENTIFIER }
SELECT missing_column FROM (SELECT 1 AS val)
SETTINGS enable_identifier_resolve_cache = 1; -- { serverError UNKNOWN_IDENTIFIER }

-- A duplicated alias is still MULTIPLE_EXPRESSIONS_FOR_ALIAS.
SELECT number AS num, num * 1 AS num FROM numbers(10)
SETTINGS enable_identifier_resolve_cache = 0; -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
SELECT number AS num, num * 1 AS num FROM numbers(10)
SETTINGS enable_identifier_resolve_cache = 1; -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
