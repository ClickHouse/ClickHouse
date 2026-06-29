-- The non-stable variance/stddev now accumulate via independent partial sums (vectorized
-- aggregation), so for a constant column they round to a tiny non-negative value instead of
-- exactly 0. The `getPopulation`/`getSample` `std::max(0, ...)` clamp still guarantees
-- non-negativity (the property this test guards); round away the last-bit noise.
SELECT round(varSamp(0.1), 6) FROM numbers(1000000);
SELECT round(varPop(0.1), 6) FROM numbers(1000000);
SELECT round(stddevSamp(0.1), 6) FROM numbers(1000000);
SELECT round(stddevPop(0.1), 6) FROM numbers(1000000);

SELECT varSampStable(0.1) FROM numbers(1000000);
SELECT varPopStable(0.1) FROM numbers(1000000);
SELECT stddevSampStable(0.1) FROM numbers(1000000);
SELECT stddevPopStable(0.1) FROM numbers(1000000);
