-- A lambda argument named after a niladic function must be hidden from an aliased expression
-- written outside of the lambda: the bare identifier resolves to the niladic function there.

SET enable_analyzer = 1;

WITH length(currentDatabase) AS n SELECT arrayMap(currentDatabase -> n, ['']) = [length(currentDatabase())];

WITH concat(currentUser, '') AS u SELECT arrayMap(currentUser -> u, ['x']) = [currentUser()];

-- A transitive alias to the bare identifier also resolves to the niladic function instead of being captured.
WITH currentDatabase AS n SELECT arrayMap(currentDatabase -> n, ['']) = [currentDatabase()];

-- The same alias referenced from a subquery: the scope walk for the alias body ends with the niladic fallback.
WITH currentDatabase AS n SELECT (SELECT n) = currentDatabase();

-- Without an alias written outside of the lambda, the argument keeps shadowing the function.
SELECT arrayMap(currentDatabase -> length(currentDatabase), ['abc']);

-- An alias owned by the lambda itself still references the argument.
SELECT arrayMap(currentDatabase -> (length(currentDatabase) AS l) + l, ['ab']);
