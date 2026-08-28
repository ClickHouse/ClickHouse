-- A lambda argument named after a niladic function must be hidden from an aliased expression
-- written outside of the lambda: the bare identifier resolves to the niladic function there.

SET enable_analyzer = 1;

WITH length(currentDatabase) AS n SELECT arrayMap(currentDatabase -> n, ['']) = [length(currentDatabase())];

WITH concat(currentUser, '') AS u SELECT arrayMap(currentUser -> u, ['x']) = [currentUser()];

-- A transitive alias to the bare identifier is refused instead of being silently captured.
WITH currentDatabase AS n SELECT arrayMap(currentDatabase -> n, ['']); -- { serverError UNKNOWN_IDENTIFIER }

-- Without an alias written outside of the lambda, the argument keeps shadowing the function.
SELECT arrayMap(currentDatabase -> length(currentDatabase), ['abc']);

-- An alias owned by the lambda itself still references the argument.
SELECT arrayMap(currentDatabase -> (length(currentDatabase) AS l) + l, ['ab']);
