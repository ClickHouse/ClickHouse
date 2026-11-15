-- Regression test: EXPLAIN with settings followed by parenthesized
-- EXCEPT/INTERSECT operands must parse correctly.
--
-- The formatter wraps EXCEPT operands in parens (to disambiguate
-- SELECT * EXCEPT col). When a setting value like `1` is immediately
-- followed by `(SELECT ...`, ParserFunction used to greedily parse
-- `1(SELECT ...)` as a function call, corrupting the parser position.

-- Without parentheses (always worked, but verify roundtrip).
SELECT formatQuery('EXPLAIN PIPELINE header = 1, compact = 1 SELECT -2 EXCEPT ALL SELECT 1');

-- With explicit parentheses — this is the form the formatter produces.
SELECT formatQuery('EXPLAIN PIPELINE header = 1, compact = 1 (SELECT -2) EXCEPT ALL (SELECT 1)');

-- Nested formatQuery proves parse → format → re-parse → re-format is stable.
SELECT formatQuery(formatQuery('EXPLAIN PIPELINE header = 1, compact = 1 (SELECT -2) EXCEPT ALL (SELECT 1)'));

-- Same for INTERSECT.
SELECT formatQuery('EXPLAIN PIPELINE header = 1, compact = 1 (SELECT -2) INTERSECT ALL (SELECT 1)');
SELECT formatQuery(formatQuery('EXPLAIN PIPELINE header = 1, compact = 1 (SELECT -2) INTERSECT ALL (SELECT 1)'));
