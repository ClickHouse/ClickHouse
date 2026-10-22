-- Test: exercises `arrayDotProduct` `combine` overflow with `NO_SANITIZE_UNDEFINED`
-- Covers: src/Functions/array/arrayDotProduct.cpp:122 — combine function with signed integer overflow
-- The PR test (03037_dot_product_overflow) only triggers overflow in `accumulate` (tail loop, array_size=2).
-- This test triggers overflow specifically in `combine` (summing per-accumulator partial sums)
-- with array_size=4 so all 4 accumulators are filled by main loop and combine sums them.
-- Each per-accumulator product fits in Int64, but their sum during combine() overflows.
select ignore(dotProduct(materialize([4611686018427387905, 4611686018427387905, 0, 0]::Array(Int64)), materialize([1, 1, 1, 1]::Array(Int64))));
