-- Test: exercises short-circuit branch in `IExecutableFunction::defaultImplementationForNulls`
-- Covers: src/Functions/IFunction.cpp:301-321 — filter / execute on filtered subset / expand path
-- This branch is only reached when `null_ratio >= short_circuit_function_evaluation_for_nulls_threshold`
-- AND `rows_without_nulls > 0` (i.e. mixed null and non-null rows with threshold < 1.0).
-- With the default threshold of 1.0 this branch is unreachable (the all-null early-return
-- at line 281 takes over), so no deterministic stateless test exercised lines 301-321 prior
-- to this test. Risk: a regression in `filter`/`expand`/`wrapInNullable(src, null_map)` could
-- silently produce wrong values for users who set `short_circuit_function_evaluation_for_nulls_threshold` < 1.0.

-- 1. concat over Nullable(String): mostly-null block, short-circuit triggered (ratio=0.75 >= 0.5)
SELECT 'concat short-circuit';
SELECT concat(materialize(if(number % 4 = 0, 'a', NULL)::Nullable(String)), materialize('b'), materialize('c'))
FROM numbers(8) ORDER BY number
SETTINGS short_circuit_function_evaluation_for_nulls = 1, short_circuit_function_evaluation_for_nulls_threshold = 0.5;

-- 2. Same query with short-circuit disabled — output MUST be identical.
SELECT 'concat baseline';
SELECT concat(materialize(if(number % 4 = 0, 'a', NULL)::Nullable(String)), materialize('b'), materialize('c'))
FROM numbers(8) ORDER BY number
SETTINGS short_circuit_function_evaluation_for_nulls = 0;

-- 3. Same query with threshold=1.0 (default): null_ratio (0.75) < threshold → no short-circuit
SELECT 'concat no short-circuit due to high threshold';
SELECT concat(materialize(if(number % 4 = 0, 'a', NULL)::Nullable(String)), materialize('b'), materialize('c'))
FROM numbers(8) ORDER BY number
SETTINGS short_circuit_function_evaluation_for_nulls = 1, short_circuit_function_evaluation_for_nulls_threshold = 1.0;

-- 4. Mostly non-null, threshold=0.0 → short-circuit forced, exercises path with few null rows
SELECT 'concat mostly non-null, threshold=0';
SELECT concat(materialize(if(number = 3, NULL, 'x')::Nullable(String)), materialize('-'), materialize('y'))
FROM numbers(8) ORDER BY number
SETTINGS short_circuit_function_evaluation_for_nulls = 1, short_circuit_function_evaluation_for_nulls_threshold = 0.0;

-- 5. Same scenario, short-circuit OFF — outputs must match line-for-line with #4.
SELECT 'concat mostly non-null baseline';
SELECT concat(materialize(if(number = 3, NULL, 'x')::Nullable(String)), materialize('-'), materialize('y'))
FROM numbers(8) ORDER BY number
SETTINGS short_circuit_function_evaluation_for_nulls = 0;

-- 6. lower over Nullable(String): unary string function — also reaches the non-numeric path
SELECT 'lower short-circuit';
SELECT lower(materialize(if(number % 4 = 0, 'HELLO', NULL)::Nullable(String)))
FROM numbers(8) ORDER BY number
SETTINGS short_circuit_function_evaluation_for_nulls = 1, short_circuit_function_evaluation_for_nulls_threshold = 0.5;

-- 7. substring with mixed-arg types and Nullable(String) — multi-arg path
SELECT 'substring short-circuit';
SELECT substring(materialize(if(number % 4 = 0, 'hello world', NULL)::Nullable(String)), materialize(1), materialize(5))
FROM numbers(8) ORDER BY number
SETTINGS short_circuit_function_evaluation_for_nulls = 1, short_circuit_function_evaluation_for_nulls_threshold = 0.5;
