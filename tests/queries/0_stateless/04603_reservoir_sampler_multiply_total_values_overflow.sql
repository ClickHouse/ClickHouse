-- Regression test: multiplying a ReservoirSampler-backed aggregate state
-- (median/quantile) by a huge constant used to hang the server.
--
-- executeAggregateMultiply self-merges the reservoir with exponentiation by
-- squaring, doubling ReservoirSampler::total_values (size_t) each step. A huge
-- multiplier overflowed the `total_values += b.total_values` sum in
-- ReservoirSampler::merge; the wrapped-small value made
-- `frequency = total_values / b.total_values` drop below 1, turning
-- `for (double i = 0; i < sample_count; i += frequency)` into a near-infinite
-- loop with no cancellation check. The fix saturates the sum on overflow.
--
-- numbers(100000) > DEFAULT_SAMPLE_COUNT (8192) so total_values > sample_count
-- and the frequency-loop branch is taken (the small-reservoir branches are not
-- affected). Before the fix these queries never terminated.

SELECT length(toString(finalizeAggregation(18446744073709551615 * (SELECT medianState(number) FROM numbers(100000)))));
SELECT length(toString(finalizeAggregation(9223372036854775806 * (SELECT medianState(number) FROM numbers(100000)))));
SELECT length(toString(finalizeAggregation(1000000000000000000 * (SELECT quantileState(0.5)(number) FROM numbers(100000)))));

-- Numerical correctness is unchanged for small multipliers: median of [0, 100000) is 50501.5.
SELECT finalizeAggregation(10 * (SELECT medianState(number) FROM numbers(100000)));
SELECT finalizeAggregation(1 * (SELECT medianState(number) FROM numbers(100000)));

-- A saturated (total_values = SIZE_MAX) state merged with a small state routes through
-- ReservoirSampler::insert (the two small-reservoir branches of merge). A plain ++total_values
-- there would wrap SIZE_MAX to 0 and reach genRandom(0) = UB / debug assert. Cover both
-- operand orders: saturated + small and small + saturated.
SELECT length(toString(finalizeAggregation((18446744073709551615 * (SELECT medianState(number) FROM numbers(100000))) + (SELECT medianState(number) FROM numbers(1)))));
SELECT length(toString(finalizeAggregation((SELECT medianState(number) FROM numbers(1)) + (18446744073709551615 * (SELECT medianState(number) FROM numbers(100000))))));
