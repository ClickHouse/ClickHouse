-- Regression test for sharing the argument's null map in `defaultImplementationForNulls`:
-- no per-row null map at all (const Nullable + non-const non-Nullable, threshold = 0),
-- sharing with a single Nullable argument, and merging two Nullable arguments.

SELECT concat(toNullable('a'), materialize('b')) FROM numbers(2)
SETTINGS short_circuit_function_evaluation_for_nulls = 1, short_circuit_function_evaluation_for_nulls_threshold = 0.0;

SELECT materialize(if(number % 2 = 0, 'AbC', NULL)::Nullable(String)) AS x, lower(x), x IS NULL FROM numbers(4) ORDER BY number;

SELECT
    materialize(if(number % 2 = 0, 'a', NULL)::Nullable(String)) AS x,
    materialize(if(number % 3 = 0, 'b', NULL)::Nullable(String)) AS y,
    concat(x, y), x IS NULL, y IS NULL
FROM numbers(6) ORDER BY number;
