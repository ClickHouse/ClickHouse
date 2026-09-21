-- The arguments of `coalesce` and `ifNull` after the leftmost non-NULL one are not evaluated.

SELECT coalesce(materialize(toNullable(1)), throwIf(materialize(1), 'Must not be evaluated'));
SELECT coalesce(materialize(toNullable(1)), throwIf(materialize(1), 'Must not be evaluated'), throwIf(materialize(2), 'Must not be evaluated'));
SELECT ifNull(materialize(toNullable(1)), throwIf(materialize(1), 'Must not be evaluated'));

-- Without short-circuit evaluation every argument is evaluated.

SELECT coalesce(materialize(toNullable(1)), throwIf(materialize(1), 'Evaluated')) SETTINGS short_circuit_function_evaluation = 'disable'; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
SELECT ifNull(materialize(toNullable(1)), throwIf(materialize(1), 'Evaluated')) SETTINGS short_circuit_function_evaluation = 'disable'; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

-- An argument is evaluated only for the rows where all the preceding arguments are NULL,
-- so the division by zero in the row number 0 does not happen.

SELECT number, coalesce(if(number = 0, toNullable(10), NULL), intDiv(100, number)) FROM numbers(4) ORDER BY number;
SELECT number, ifNull(if(number = 0, toNullable(10), NULL), intDiv(100, number)) FROM numbers(4) ORDER BY number;
SELECT number, coalesce(if(number = 0, toNullable(10), NULL), if(number = 1, toNullable(20), NULL), intDiv(100, number)) FROM numbers(4) ORDER BY number;

-- The same inside a branch of another short-circuit function, where `coalesce` itself is evaluated lazily.

SELECT number, if(number > 0, coalesce(if(number = 1, toNullable(10), NULL), intDiv(100, number - 1)), 0) FROM numbers(3) ORDER BY number;
SELECT number, if(number > 0, ifNull(if(number = 1, toNullable(10), NULL), intDiv(100, number - 1)), 0) FROM numbers(3) ORDER BY number;

-- A lazily executed argument is needed for a part of the rows.

SELECT number, coalesce(if(number % 2, toNullable(number), NULL), intDiv(100, number + 1)) FROM numbers(4) ORDER BY number;
SELECT number, ifNull(if(number % 2, toNullable(number), NULL), intDiv(100, number + 1)) FROM numbers(4) ORDER BY number;

-- All the arguments are NULL.

SELECT number, coalesce(CAST(NULL, 'Nullable(UInt8)'), if(number % 2, NULL, CAST(NULL, 'Nullable(UInt8)'))) FROM numbers(2) ORDER BY number;

-- Short-circuit evaluation does not change the result.

SELECT sum(coalesce(if(number % 3 = 0, toNullable(number), NULL), if(number % 3 = 1, toNullable(number * 2), NULL), intDiv(1000, number + 1))) FROM numbers(1000) SETTINGS short_circuit_function_evaluation = 'enable';
SELECT sum(coalesce(if(number % 3 = 0, toNullable(number), NULL), if(number % 3 = 1, toNullable(number * 2), NULL), intDiv(1000, number + 1))) FROM numbers(1000) SETTINGS short_circuit_function_evaluation = 'disable';

SELECT sum(cityHash64(ifNull(if(number % 2, toNullable(number), NULL), intDiv(1000, number + 1)))) FROM numbers(1000) SETTINGS short_circuit_function_evaluation = 'enable';
SELECT sum(cityHash64(ifNull(if(number % 2, toNullable(number), NULL), intDiv(1000, number + 1)))) FROM numbers(1000) SETTINGS short_circuit_function_evaluation = 'disable';

SELECT sum(cityHash64(coalesce(if(number % 3 = 0, toLowCardinality(toNullable(toString(number))), NULL), toLowCardinality(toString(intDiv(1000, number + 1)))))) FROM numbers(1000) SETTINGS short_circuit_function_evaluation = 'enable';
SELECT sum(cityHash64(coalesce(if(number % 3 = 0, toLowCardinality(toNullable(toString(number))), NULL), toLowCardinality(toString(intDiv(1000, number + 1)))))) FROM numbers(1000) SETTINGS short_circuit_function_evaluation = 'disable';
