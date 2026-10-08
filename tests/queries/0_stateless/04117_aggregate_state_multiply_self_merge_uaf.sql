-- Tags: no-fasttest
-- ^ The reproducer needs Address Sanitizer / Memory Sanitizer to surface the
--   bug; release builds happen to read freed memory without aborting, so a
--   fast-test-only run would be silent.

-- Regression test for STID 0988-40af: heap-use-after-free in
-- FunctionBinaryArithmetic::executeAggregateMultiply, triggered when the
-- exponentiation-by-squaring loop calls func->merge(state, state) on an
-- aggregate function whose merge implementation appends the source's internal
-- storage into the destination's. With src == dst, the source iterators are
-- invalidated by the destination's reallocation, producing a heap-use-after-
-- free. The fix copies the state to an independent column before each
-- squaring step.

-- groupArrayState's merge appends rhs.value to lhs.value (a PODArray). With
-- src == dst the source iterator points to a buffer that is reallocated mid-
-- merge, which deterministically reproduces the heap-use-after-free under
-- Address Sanitizer. The result length grows linearly with the multiplier:
SELECT length(finalizeAggregation(state * 1))   FROM (SELECT groupArrayState(number) AS state FROM numbers(50));
SELECT length(finalizeAggregation(state * 2))   FROM (SELECT groupArrayState(number) AS state FROM numbers(50));
SELECT length(finalizeAggregation(state * 4))   FROM (SELECT groupArrayState(number) AS state FROM numbers(50));
SELECT length(finalizeAggregation(state * 8))   FROM (SELECT groupArrayState(number) AS state FROM numbers(50));
SELECT length(finalizeAggregation(state * 16))  FROM (SELECT groupArrayState(number) AS state FROM numbers(50));
SELECT length(finalizeAggregation(state * 256)) FROM (SELECT groupArrayState(number) AS state FROM numbers(50));

-- Pure-odd and mixed multipliers still exercise the squaring branch (e.g.
-- 7 = 4 + 2 + 1 squares twice).
SELECT length(finalizeAggregation(state * 3)) FROM (SELECT groupArrayState(number) AS state FROM numbers(50));
SELECT length(finalizeAggregation(state * 5)) FROM (SELECT groupArrayState(number) AS state FROM numbers(50));
SELECT length(finalizeAggregation(state * 7)) FROM (SELECT groupArrayState(number) AS state FROM numbers(50));

-- quantilesExactState is the variant the production fuzzer caught (its
-- internal PODArray<Value, bytes_in_arena> is what the original sanitizer
-- report flagged). The aggregate result is a fixed-length quantiles array,
-- but the squaring path still allocates / merges the underlying buffer the
-- same way.
SELECT length(finalizeAggregation(state * 8)) FROM (SELECT quantilesExactState(0.5)(number) AS state FROM numbers(50));

-- Constant column path: a single state value replicated across multiple
-- rows. agg_state_is_const takes the size=1 branch in executeAggregateMultiply.
SELECT length(finalizeAggregation(arrayJoin([state, state]) * 4))
FROM (SELECT groupArrayState(number) AS state FROM numbers(50));

-- Numerical correctness: multiplying the state by N should produce N*size
-- elements; sum-aggregating them gives N * sum([0..49]) = N * 1225.
SELECT sum(arrayJoin(finalizeAggregation(state * 4)))
FROM (SELECT groupArrayState(number) AS state FROM numbers(50));
