-- { echo }

-- This is to ensure runtime `max_rows_to_read` checks by the pipeline are not flaky
SET max_block_size = 8192;

-- No-filter, bounded source — primes(N) without WHERE
SELECT * FROM primes(1e19) SETTINGS max_rows_to_read = 1e18 FORMAT Hash; -- { serverError TOO_MANY_ROWS }
SELECT * FROM primes(2000) SETTINGS max_rows_to_read = 1999 FORMAT Hash; -- { serverError TOO_MANY_ROWS }

SELECT * FROM primes(2000) SETTINGS max_rows_to_read = 2000 FORMAT Hash;

-- With large offset (the sieve must traverse offset + (limit-1)*step + 1 primes)
SELECT * FROM primes(1e19, 1e19) SETTINGS max_rows_to_read = 2000 FORMAT Hash; -- { serverError TOO_MANY_ROWS }
SELECT * FROM primes(2000, 6) SETTINGS max_rows_to_read = 2005 FORMAT Hash; -- { serverError TOO_MANY_ROWS }

SELECT * FROM primes(2000, 6) SETTINGS max_rows_to_read = 2006 FORMAT Hash;

-- With large step (index span = offset + (limit-1)*step + 1 can be much larger than limit, or even saturate to UInt64 max)
SELECT * FROM primes(1e19, 1e19, 1e19) SETTINGS max_rows_to_read = 2000 FORMAT Hash; -- { serverError TOO_MANY_ROWS }
SELECT * FROM primes(0, 1e19, 1e19) SETTINGS max_rows_to_read = 2000 FORMAT Hash; -- { serverError TOO_MANY_ROWS }
SELECT * FROM primes(0, 1e15, 1e15) SETTINGS max_rows_to_read = 2000 FORMAT Hash; -- { serverError TOO_MANY_ROWS }
SELECT * FROM primes(0, 10, 500) SETTINGS max_rows_to_read = 4500 FORMAT Hash; -- { serverError TOO_MANY_ROWS }
SELECT * FROM primes(0, 2, 5000) SETTINGS max_rows_to_read = 2 FORMAT Hash; -- { serverError TOO_MANY_ROWS }

SELECT * FROM primes(0, 10, 500) SETTINGS max_rows_to_read = 4501 FORMAT Hash;

-- No-filter, unbounded source — system.primes LIMIT N
SELECT * FROM system.primes LIMIT 2000 SETTINGS max_rows_to_read = 1999 FORMAT Hash; -- { serverError TOO_MANY_ROWS }

SELECT * FROM system.primes LIMIT 2000 SETTINGS max_rows_to_read = 2000 FORMAT Hash;

-- ExactRanges, simple (offset=0, step=1) — unbounded + exact range filter
SELECT * FROM system.primes WHERE prime BETWEEN 2 AND 50000 SETTINGS max_rows_to_read = 5132 FORMAT Hash; -- { serverError TOO_MANY_ROWS }

SELECT * FROM system.primes WHERE prime BETWEEN 2 AND 50000 SETTINGS max_rows_to_read = 5133 FORMAT Hash;

-- ExactRanges, simple + LIMIT — unbounded + exact range + query LIMIT
SELECT * FROM system.primes WHERE prime BETWEEN 2 AND 50000 LIMIT 10 SETTINGS max_rows_to_read = 9 FORMAT Hash; -- { serverError TOO_MANY_ROWS }

SELECT * FROM system.primes WHERE prime BETWEEN 2 AND 50000 LIMIT 100000 SETTINGS max_rows_to_read = 5132 FORMAT Hash; -- { serverError TOO_MANY_ROWS }

SELECT * FROM system.primes WHERE prime BETWEEN 2 AND 50000 LIMIT 10 SETTINGS max_rows_to_read = 10 FORMAT Hash;

-- Arbitrarily high limit does not impact rows generated for exact ranges and should not cause TOO_MANY_ROWS if max_rows_to_read is sufficient.
SELECT * FROM system.primes WHERE prime BETWEEN 2 AND 50000 LIMIT 100000 SETTINGS max_rows_to_read = 5133 FORMAT Hash;

-- ConservativeRanges, simple — unbounded + non-exact filter with bounds
SELECT * FROM system.primes WHERE prime < 50000 AND prime % 10 = 1 SETTINGS max_rows_to_read = 9 FORMAT Hash; -- { serverError TOO_MANY_ROWS }

SELECT * FROM system.primes WHERE prime < 50000 AND prime % 10 = 1 SETTINGS max_rows_to_read = 5132 FORMAT Hash; -- { serverError TOO_MANY_ROWS }

SELECT * FROM system.primes WHERE prime < 50000 AND prime % 10 = 1 SETTINGS max_rows_to_read = 5133 FORMAT Hash;

-- Fallback, bounded — primes(N) with WHERE clause
SELECT * FROM primes(2000) WHERE prime > 10 SETTINGS max_rows_to_read = 1999 FORMAT Hash; -- { serverError TOO_MANY_ROWS }

SELECT * FROM primes(2000) WHERE prime > 1 LIMIT 2 SETTINGS max_rows_to_read = 1999, max_block_size = 2000 FORMAT Hash; -- { serverError TOO_MANY_ROWS }

-- Since we are doing runtime check, max_block_size should also be considered as we check the rows read after each block.
SELECT * FROM primes(2000) WHERE prime > 1 LIMIT 2 SETTINGS max_rows_to_read = 1999, max_block_size = 1999 FORMAT Hash;

SELECT * FROM primes(2000) WHERE prime > 1 LIMIT 2 SETTINGS max_rows_to_read = 2000, max_block_size = 2000 FORMAT Hash;

SELECT * FROM primes(2000) WHERE prime > 1 LIMIT 2 SETTINGS max_rows_to_read = 2000, max_block_size = 1999 FORMAT Hash;

SELECT * FROM primes(2000) WHERE prime > 1 LIMIT 2 SETTINGS max_rows_to_read = 2000, max_block_size = 10 FORMAT Hash;

-- Bounded with large offset + WHERE (fallback path, index span matters).
SELECT * FROM primes(1, 2000, 1) WHERE prime > 1 LIMIT 2 SETTINGS max_rows_to_read = 2000, max_block_size = 10 FORMAT Hash; -- { serverError TOO_MANY_ROWS }

SELECT * FROM primes(1, 2000, 1) WHERE prime > 1 LIMIT 2 SETTINGS max_rows_to_read = 2000, max_block_size = 20000 FORMAT Hash;  -- { serverError TOO_MANY_ROWS }

SELECT * FROM primes(5000, 6, 100) WHERE prime > 10 SETTINGS max_rows_to_read = 1000 FORMAT Hash; -- { serverError TOO_MANY_ROWS }

SELECT * FROM primes(5000, 6, 100) WHERE prime > 10 SETTINGS max_rows_to_read = 1000000 FORMAT Hash;

-- Fallback, unbounded — system.primes with non-interval predicate
-- No effective_limit, enforced at runtime by the pipeline
SELECT * FROM system.primes WHERE bitAnd(prime, prime + 1) = 0 SETTINGS max_rows_to_read = 1000 FORMAT Hash; -- { serverError TOO_MANY_ROWS }

SELECT * FROM system.primes WHERE bitAnd(prime, prime + 1) = 0 LIMIT 4 SETTINGS max_rows_to_read = 100000 FORMAT Hash;
