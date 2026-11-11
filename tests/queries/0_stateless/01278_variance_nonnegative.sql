-- Variance must never be negative: rounding of the raw power sums can make it slightly
-- negative for constant data, and it is clamped to zero from below. The rounding, and with
-- it the sign of the error, depends on the summation order (block size, unrolling), so the
-- non-Stable variants are only near zero; assert the invariant instead of the exact value.
SELECT varSamp(0.1) BETWEEN 0 AND 1e-6 FROM numbers(1000000);
SELECT varPop(0.1) BETWEEN 0 AND 1e-6 FROM numbers(1000000);
SELECT stddevSamp(0.1) BETWEEN 0 AND 1e-3 FROM numbers(1000000);
SELECT stddevPop(0.1) BETWEEN 0 AND 1e-3 FROM numbers(1000000);

SELECT varSampStable(0.1) FROM numbers(1000000);
SELECT varPopStable(0.1) FROM numbers(1000000);
SELECT stddevSampStable(0.1) FROM numbers(1000000);
SELECT stddevPopStable(0.1) FROM numbers(1000000);
