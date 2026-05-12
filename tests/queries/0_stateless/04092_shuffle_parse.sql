-- Test that SHUFFLE parses and round-trips correctly.
-- Uses formatQuery() which is a pure parser+formatter (no Analyzer rewrite).

-- Basic SHUFFLE
SELECT formatQuery('SELECT number FROM numbers(10) SHUFFLE');

-- SHUFFLE with LIMIT
SELECT formatQuery('SELECT number FROM numbers(10) SHUFFLE LIMIT 5');

-- SHUFFLE with WHERE
SELECT formatQuery('SELECT number % 3 AS r FROM numbers(10) WHERE number > 0 SHUFFLE LIMIT 3');

-- SHUFFLE in subquery, outer ORDER BY (different scopes — both valid)
SELECT formatQuery('SELECT * FROM (SELECT number FROM numbers(10) SHUFFLE) ORDER BY number');

-- SHUFFLE and ORDER BY are mutually exclusive: ORDER BY first, SHUFFLE token follows
SELECT number FROM numbers(5) ORDER BY number SHUFFLE; -- { serverError SYNTAX_ERROR }

-- SHUFFLE and ORDER BY are mutually exclusive: SHUFFLE first, ORDER BY left as trailing garbage
SELECT number FROM numbers(5) SHUFFLE ORDER BY number; -- { serverError SYNTAX_ERROR }
