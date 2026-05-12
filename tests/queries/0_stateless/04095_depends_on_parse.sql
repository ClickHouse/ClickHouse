-- PR 4: Parser tests for ORDER BY col DEPENDS ON dep_col.
-- Verifies round-trip parsing; no execution is tested.

-- Basic round-trip: DEPENDS ON preserved by formatQuery.
SELECT formatQuery('SELECT x FROM t ORDER BY x DEPENDS ON deps');

-- DEPENDS ON with LIMIT coexist.
SELECT formatQuery('SELECT x FROM t ORDER BY x DEPENDS ON deps LIMIT 10');

-- DEPENDS ON in a subquery; outer query has a normal ORDER BY.
SELECT formatQuery('SELECT * FROM (SELECT x, d FROM t ORDER BY x DEPENDS ON d) ORDER BY x');

-- formatQuery output contains DEPENDS ON.
SELECT position(formatQuery('SELECT x FROM t ORDER BY x DEPENDS ON deps'), 'DEPENDS ON') > 0 AS has_depends_on;

-- Error: DEPENDS ON + ASC
SELECT 1 ORDER BY x ASC DEPENDS ON deps; -- { serverError SYNTAX_ERROR }

-- Error: DEPENDS ON + DESC
SELECT 1 ORDER BY x DESC DEPENDS ON deps; -- { serverError SYNTAX_ERROR }

-- Error: DEPENDS ON with multiple ORDER BY elements
SELECT 1 ORDER BY x DEPENDS ON deps, y; -- { serverError SYNTAX_ERROR }

-- Error: DEPENDS ON + COLLATE (COLLATE parsed before DEPENDS ON, then error thrown)
SELECT 1 ORDER BY x COLLATE 'en' DEPENDS ON deps; -- { serverError SYNTAX_ERROR }
