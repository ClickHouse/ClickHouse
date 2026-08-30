-- Tags: no-parallel
-- Reason for no-parallel: this test creates SQL UDFs (`test_04064_double`,
-- `test_04064_add`); concurrent runs in flaky check would race on
-- `CREATE FUNCTION` and `DROP FUNCTION` for these global names.

-- Test passing bare function names to higher-order functions instead of lambdas.
-- https://github.com/ClickHouse/ClickHouse/issues/63498

-- The bare-function-to-lambda rewrite is implemented only in the new analyzer.
SET enable_analyzer = 1;

-- Basic: arrayMap with a function name
SELECT arrayMap(negate, [1, 2, 3]);
SELECT arrayMap(toString, [1, 2, 3]);
SELECT arrayMap(toUInt64, [1.1, 2.2, 3.3]);
SELECT arrayMap(length, ['hello', 'world', '!']);

-- arrayFilter with a function name
SELECT arrayFilter(isNotNull, [1, NULL, 3, NULL, 5]);

-- arrayExists / arrayAll
SELECT arrayExists(isNull, [1, NULL, 3]);
SELECT arrayAll(isNotNull, [1, 2, 3]);

-- Multiple array arguments (binary function as lambda)
SELECT arrayMap(plus, [1, 2, 3], [10, 20, 30]);
SELECT arrayMap(multiply, [1, 2, 3], [10, 20, 30]);

-- arrayFold with a bare function name (accumulator + element)
SELECT arrayFold(plus, [1, 2, 3, 4, 5], toUInt64(0));

-- Nested higher-order functions
SELECT arrayMap(toString, arrayMap(negate, [1, 2, 3]));

-- arrayPartialSort with a bare function name (has a fixed `limit` parameter).
-- Only the first `limit` elements are guaranteed to be sorted; take just those
-- to keep the reference deterministic.
SELECT arraySlice(arrayPartialSort(negate, 2, [5, 9, 1, 3]), 1, 2);

-- arraySort with a bare function name
SELECT arraySort(negate, [3, 1, 4, 1, 5]);

-- Backward compatibility: column name takes priority over function name
SELECT arrayMap(x, [1, 2, 3]) FROM (SELECT 1 AS x); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Ensure non-higher-order functions don't get the transformation.
-- `plus` is not a higher-order function, so `isHigherOrderFunction` is false and
-- the rewrite is skipped. `plus(1, 2)` evaluates normally to 3.
SELECT plus(1, 2);

-- Tuple-destructuring: passing `plus` (fixed arity 2) to `arrayMap` with a single
-- array of 2-element tuples is rewritten to `arrayMap((x0, x1) -> plus(x0, x1), ...)`,
-- which `arrayMap` destructures across the tuple elements.
SELECT arrayMap(plus, [(1, 10), (2, 20), (3, 30)]);

-- Variadic inner functions on tuple inputs require an explicit lambda.
-- `arrayMap(concat, [('a','b'), ('c','d')])` would be rewritten as a unary
-- lambda and is not equivalent to `(x, y) -> concat(x, y)` after tuple
-- destructuring, so write the explicit lambda instead.
SELECT arrayMap((x, y) -> concat(x, y), [('a', 'b'), ('c', 'd')]);

-- A non-HOF parent rejects bare function names even if they would otherwise be valid.
-- `plus` is not a higher-order function, so `plus(negate, 1)` is not rewritten and
-- fails because `negate` cannot be resolved as a column/alias.
SELECT plus(negate, 1); -- { serverError UNKNOWN_IDENTIFIER }

-- Non-HOF function with a function name as argument: should not be rewritten
SELECT length(toString(123));

-- SQL UDF: unary
CREATE FUNCTION test_04064_double AS x -> x * 2;
SELECT arrayMap(test_04064_double, [1, 2, 3]);
DROP FUNCTION test_04064_double;

-- SQL UDF: binary
CREATE FUNCTION test_04064_add AS (x, y) -> x + y;
SELECT arrayMap(test_04064_add, [1, 2, 3], [10, 20, 30]);
DROP FUNCTION test_04064_add;

-- Executable UDF: `test_function` is configured via `tests/config/test_function.xml`
-- and sums two UInt64 arguments. This confirms that the rewrite reaches
-- `UserDefinedExecutableFunctionFactory` and not only `FunctionFactory`/SQL UDFs.
SELECT arrayMap(test_function, [toUInt64(1), toUInt64(2), toUInt64(3)], [toUInt64(10), toUInt64(20), toUInt64(30)]);

-- Column/alias takes priority over an executable UDF that requires command parameters.
-- `test_function_with_parameter` is configured with a `{param:UInt64}` placeholder, so
-- the candidate-resolver lookup inside the rewrite must not throw `BAD_ARGUMENTS` when
-- instantiated with empty parameters; otherwise the alias would never get a chance to
-- resolve. The query selects the alias as the function argument, which fails type
-- checking — confirming the alias won, not the UDF.
SELECT arrayMap(test_function_with_parameter, [1, 2, 3]) FROM (SELECT 1 AS test_function_with_parameter); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Fixed-arity zero-argument inner function: applying a 0-arg function to lambda
-- arguments makes no sense, so the rewrite is skipped (rather than building an
-- invalid `x -> UTCTimestamp(x)` lambda). `UTCTimestamp` then remains as an
-- unresolvable identifier in expression context.
SELECT arrayMap(UTCTimestamp, [1, 2, 3]); -- { serverError UNKNOWN_IDENTIFIER }

-- `map*Like` functions (`mapContainsKeyLike`, `mapContainsValueLike`, `mapExtractKeyLike`,
-- `mapExtractValueLike`) take `(Map, String pattern)` at SQL level and synthesise the
-- lambda internally; their first argument is not a lambda, so the bare-function rewrite
-- must not trigger. The functions themselves still work with their normal arguments.
SELECT mapContainsKeyLike(map('apple', 1, 'banana', 2), 'a%');
SELECT mapContainsValueLike(map('a', 'apple', 'b', 'banana'), 'a%');
SELECT mapExtractKeyLike(map('apple', 1, 'banana', 2, 'avocado', 3), 'a%') = map('apple', 1, 'avocado', 3);
SELECT mapExtractValueLike(map('a', 'apple', 'b', 'banana', 'c', 'avocado'), 'a%') = map('a', 'apple', 'c', 'avocado');

-- Passing a bare function name as the first argument to `mapContainsKeyLike` must not be
-- rewritten into a lambda (the first SQL argument is a Map, not a lambda). The identifier
-- `negate` fails to resolve as a column/alias, which is the expected diagnostic — proving
-- the rewrite was skipped rather than producing a confusing lambda-conversion error.
SELECT mapContainsKeyLike(negate, 'a%'); -- { serverError UNKNOWN_IDENTIFIER }
