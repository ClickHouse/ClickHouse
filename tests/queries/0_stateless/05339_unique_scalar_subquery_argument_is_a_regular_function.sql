-- `unique(...)` is parsed as the `UNIQUE` predicate only when its whole argument is a subquery.
-- A call whose first argument merely starts with a parenthesized scalar subquery is an ordinary function call.

SELECT formatQuerySingleLine('SELECT unique((SELECT 1), 2)');
SELECT formatQuerySingleLine('SELECT unique((SELECT 1) + 1)');
SELECT formatQuerySingleLine('SELECT unique(((SELECT 1)), [1, 2])');
SELECT unique((SELECT 1), 2); -- { serverError UNKNOWN_FUNCTION }
SELECT unique((SELECT 1) + 1); -- { serverError UNKNOWN_FUNCTION }

-- These are still the predicate.
SELECT formatQuerySingleLine('SELECT unique(((SELECT 1)))');
SELECT formatQuerySingleLine('SELECT unique((SELECT 1) UNION ALL (SELECT 2))');
SELECT formatQuerySingleLine('SELECT unique(SELECT 1, 2)');
SELECT UNIQUE((SELECT 1) UNION ALL (SELECT 1)), UNIQUE((SELECT 1) UNION ALL (SELECT 2)), UNIQUE(((SELECT number FROM numbers(3))));
