-- Tags: no-parallel
-- ^ creates a user defined function, which is global.

-- The `f | g` operator does not reserve the name it parses into: `__compose` is a legal
-- identifier, and only the operator syntax makes a node a composition.
DROP FUNCTION IF EXISTS __compose;
CREATE FUNCTION __compose AS (x, y) -> x + y;
SELECT __compose(1, 2);
SELECT arrayMap(__compose(_1, 1), [1, 2]);
-- The operator keeps working while such a function exists.
SELECT arrayMap(plus(_, 1) | multiply(_, 2), [1, 2, 3]);
DROP FUNCTION __compose;

-- A name bound in the query keeps priority over the placeholder syntax, including a lambda
-- bound with `WITH`, which lives in the function lookup rather than the expression lookup.
WITH (x -> x + 10) AS _1 SELECT arrayMap(_1, [1, 2]);
WITH (x -> x + 10) AS _1 SELECT arrayMap(_1 | toString, [1, 2]);
WITH (x -> x + 10) AS _ SELECT arrayMap(_, [1, 2]);

-- The alias of a table expression binds a qualified name, but not a bare identifier, so a free
-- bare occurrence of the substituted name inside a scalar subquery is still rejected, while a
-- member access on the alias is local to the subquery and needs no substitution.
SELECT arrayMap((x -> x + 1) | (y -> y + (SELECT y FROM (SELECT 1 AS z) AS y)), [1]); -- { serverError NOT_IMPLEMENTED }
SELECT arrayMap((x -> x + 1) | (y -> y + (SELECT y.z FROM (SELECT 1 AS z) AS y)), [1]);
-- An unrelated alias in the subquery does not suppress the guard either.
SELECT arrayMap((x -> x + 1) | (y -> y + (SELECT y FROM (SELECT 1 AS z) AS t)), [1]); -- { serverError NOT_IMPLEMENTED }
