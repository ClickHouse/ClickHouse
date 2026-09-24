-- Tests error branches in FunctionsStringSimilarity.h and
-- FunctionMathBinaryFloat64.h that had zero CI coverage.
--
-- COVERAGE TARGETS
--
--   src/Functions/FunctionsStringSimilarity.h, lines 129-137:
--     When the haystack is a constant string and the needle is a vector column,
--     ngramDistance checks whether the haystack exceeds max_string_size (32768)
--     before building the ngram table. This const-vector haystack path was never
--     exercised: test 05182 covers needle-too-large (const-const and vector-const),
--     but not haystack-too-large (const-vector). Without the check, a haystack of
--     arbitrary size would be passed to NgramDistanceImpl, which allocates a
--     fixed-size stack buffer indexed by the haystack length, causing a stack
--     overflow for inputs > 32768 bytes.
--
--   src/Functions/FunctionMathBinaryFloat64.h, lines 42-44:
--     All two-argument floating-point math functions (pow, atan2, log, hypot,
--     etc.) share a getReturnTypeImpl that rejects arguments whose type is not a
--     native number, decimal, or bfloat16. This guard was never triggered in CI
--     because every existing test passes numeric types. Without the guard, a
--     String or Date argument would reach the JIT execution layer unchecked.

-- Haystack is a 32769-byte constant (exceeds max_string_size = 32768); needle is
-- a materialised column. Exercises FunctionsStringSimilarity.h lines 129-137.
SELECT ngramDistance(repeat('x', 32769), materialize('hello')); -- { serverError TOO_LARGE_STRING_SIZE }

-- String as first argument to pow. Exercises FunctionMathBinaryFloat64.h lines 42-44.
SELECT pow('hello'::String, 2); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- String as second argument to pow (the same guard checks both arguments).
SELECT pow(2, 'world'::String); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
