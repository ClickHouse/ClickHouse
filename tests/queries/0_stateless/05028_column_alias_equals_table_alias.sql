-- Tests for https://github.com/ClickHouse/ClickHouse/issues/79966
-- A column alias may coincide with the alias of a table expression in the FROM section.

SET enable_analyzer = 1;

SELECT number AS test_name
FROM (SELECT * FROM numbers(3)) AS test_name
ORDER BY test_name;

SELECT '---';

-- The alias still qualifies columns of the table expression.
SELECT number AS test_name, test_name.number AS qualified
FROM (SELECT * FROM numbers(3)) AS test_name
ORDER BY number;

SELECT '---';

-- The alias can be used as a qualifier in the JOIN condition.
SELECT number AS test_name
FROM (SELECT * FROM numbers(3)) AS test_name
INNER JOIN (SELECT * FROM numbers(3)) AS other ON test_name.number = other.number
ORDER BY test_name.number;

SELECT '---';

-- The variant from the issue: the qualifier of the projection column coincides with the alias of another table expression.
SELECT resultIfTrue_left_operand_result.number AS resultIfTrue
FROM (SELECT * FROM numbers(3)) AS resultIfTrue
LEFT JOIN (SELECT * FROM numbers(3)) AS resultIfFalse ON resultIfTrue.number = resultIfFalse.number
LEFT JOIN (SELECT * FROM numbers(3)) AS resultIfTrue_left_operand_result ON resultIfFalse.number = resultIfTrue_left_operand_result.number
ORDER BY 1;

SELECT '---';

-- The alias of an ARRAY JOIN result column coincides with the alias of the table expression.
SELECT arr AS q FROM (SELECT [1, 2] AS arr0) AS q ARRAY JOIN arr0 AS arr ORDER BY arr;

SELECT '---';

-- The same alias in nested scopes.
SELECT number AS t FROM (SELECT number FROM numbers(2) AS t) AS t ORDER BY t;

SELECT '---';

-- An identifier alias from the WITH section loses to the table expression alias from the FROM section.
WITH dummy AS x SELECT b FROM (SELECT 7 AS b) AS x;

SELECT '---';

-- Duplicate aliases of two table expressions are still rejected.
SELECT 1 FROM (SELECT 1) AS x, (SELECT 2) AS x; -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
