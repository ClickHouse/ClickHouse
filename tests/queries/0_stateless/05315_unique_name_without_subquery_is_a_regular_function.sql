-- `UNIQUE(subquery)` is a predicate, but `unique` must stay a regular name for any other call shape:
-- the parser must not reserve it, so user-defined functions and parameterized views named `unique` keep working.

-- A call without a subquery is an ordinary function call, not a syntax error.
SELECT formatQuerySingleLine('SELECT unique(1)');
SELECT formatQuerySingleLine('SELECT unique(select)');
SELECT formatQuerySingleLine('SELECT unique(from, 1)');
SELECT unique(1); -- { serverError UNKNOWN_FUNCTION }

-- The subquery form, including the parenthesized one, is still the predicate.
SELECT formatQuerySingleLine('SELECT unique(SELECT 1)');
SELECT formatQuerySingleLine('SELECT unique((SELECT 1))');
SELECT UNIQUE(SELECT number FROM numbers(3)), UNIQUE((SELECT number % 2 FROM numbers(4)));

-- A parameterized view named `unique`.
DROP TABLE IF EXISTS unique;
CREATE VIEW unique AS SELECT number FROM numbers(10) WHERE number = {x:UInt64};
SELECT * FROM unique(x = 3);
SELECT number FROM unique(x = 5) WHERE UNIQUE(SELECT number FROM numbers(3));
DROP TABLE unique;
