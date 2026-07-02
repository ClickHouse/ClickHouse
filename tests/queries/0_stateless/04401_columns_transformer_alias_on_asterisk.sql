-- Regression test: a column transformer that assigns an alias to its result
-- (APPLY with a name prefix, or REPLACE) crashed with "Can't set alias of ...
-- of QualifiedAsterisk" (LOGICAL_ERROR, server abort in debug/sanitizer builds)
-- when the transformer expression expanded to an asterisk / qualified asterisk /
-- COLUMNS matcher. Now gives a clear BAD_ARGUMENTS error.
-- https://github.com/ClickHouse/ClickHouse/issues/109214

-- Old analyzer: setting a name/alias on an expanded asterisk is rejected cleanly.
SET enable_analyzer = 0;

-- APPLY lambda whose body is a qualified asterisk, with a name prefix (the fuzzer query).
SELECT * APPLY (x -> compound_value.*, 'f_') FROM (SELECT 1 AS a); -- { serverError BAD_ARGUMENTS }
-- APPLY lambda whose body is a plain asterisk, with a name prefix.
SELECT * APPLY (x -> *, 'f_') FROM (SELECT 1 AS a); -- { serverError BAD_ARGUMENTS }
-- APPLY lambda whose body is a COLUMNS matcher, with a name prefix.
SELECT * APPLY (x -> COLUMNS('a'), 'f_') FROM (SELECT 1 AS a); -- { serverError BAD_ARGUMENTS }
-- REPLACE whose replacement expression is a qualified asterisk.
SELECT * REPLACE (compound_value.* AS a) FROM (SELECT 1 AS a); -- { serverError BAD_ARGUMENTS }

-- Valid transformers must keep working (old analyzer).
SELECT * APPLY (toString, 'f_') FROM (SELECT 1 AS a, 2 AS b) FORMAT TSVWithNames;
SELECT * APPLY (x -> x + 1, 'f_') FROM (SELECT 1 AS a, 2 AS b) FORMAT TSVWithNames;
SELECT * APPLY (x -> x + 1, 'p_') APPLY (x -> x + 1, 'q_') FROM (SELECT 1 AS a) FORMAT TSVWithNames;
SELECT * REPLACE (a + 1 AS a) FROM (SELECT 1 AS a, 2 AS b) FORMAT TSVWithNames;

-- New (default) analyzer: the APPLY name prefix must be applied consistently with the
-- old analyzer (it used to be silently dropped). https://github.com/ClickHouse/ClickHouse/pull/109223
SET enable_analyzer = 1;

SELECT * APPLY (toString, 'f_') FROM (SELECT 1 AS a, 2 AS b) FORMAT TSVWithNames;
SELECT * APPLY (x -> x + 1, 'f_') FROM (SELECT 1 AS a, 2 AS b) FORMAT TSVWithNames;
SELECT * APPLY (x -> x + 1, 'p_') APPLY (x -> x + 1, 'q_') FROM (SELECT 1 AS a) FORMAT TSVWithNames;
SELECT * REPLACE (a + 1 AS a) FROM (SELECT 1 AS a, 2 AS b) FORMAT TSVWithNames;
-- The prefix survives an EXPLAIN QUERY TREE round-trip (toAST reconstructs it).
SELECT count() FROM (EXPLAIN QUERY TREE SELECT * APPLY (toString, 'f_') FROM (SELECT 1 AS a)) WHERE explain ILIKE '%f_a%';
