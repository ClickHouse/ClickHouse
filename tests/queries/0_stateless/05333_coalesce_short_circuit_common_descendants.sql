-- A subexpression shared by several arguments of `coalesce` after the first one
-- is evaluated only for the rows where all the preceding arguments are NULL.

SELECT number, coalesce(if(number = 0, toNullable(1), NULL), throwIf(number = 0, 'dead') + 1, throwIf(number = 0, 'dead') + 2) FROM numbers(2) ORDER BY number;
SELECT number, coalesce(if(number = 0, toNullable(10), NULL), if(number = 1, toNullable(intDiv(100, number)), NULL), intDiv(100, number) + 1) FROM numbers(3) ORDER BY number;

-- The shared subexpression is still evaluated for the rows where it is needed.

SELECT number, coalesce(if(number % 2, toNullable(number), NULL), if(number % 3 = 0, toNullable(intDiv(100, number + 1)), NULL), intDiv(100, number + 1) * 2) FROM numbers(6) ORDER BY number;

-- With a leading constant NULL argument.

SELECT number, coalesce(NULL, if(number = 0, toNullable(1), NULL), throwIf(number = 0, 'dead') + 1, throwIf(number = 0, 'dead') + 2) FROM numbers(2) ORDER BY number;

-- Without short-circuit evaluation every argument is evaluated.

SELECT coalesce(if(number = 0, toNullable(1), NULL), throwIf(number = 0, 'dead') + 1, throwIf(number = 0, 'dead') + 2) FROM numbers(2) SETTINGS short_circuit_function_evaluation = 'disable'; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
