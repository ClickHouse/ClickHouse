-- Verify that primes() with a huge offset is rejected by max_rows_to_read
-- rather than running the sieve for an astronomical time.
-- This was found by the fuzzer: SELECT * FROM primes(gccMurmurHash('\0'), 6)

SELECT * FROM primes(1000000000, 6) SETTINGS max_rows_to_read = 1000000; -- { serverError TOO_MANY_ROWS }
SELECT * FROM primes(1000000000, 6, 100) SETTINGS max_rows_to_read = 1000000; -- { serverError TOO_MANY_ROWS }

-- Large step that causes index_span to saturate should also be rejected.
SELECT * FROM primes(0, 1000000000000000000, 1000000000000000000) SETTINGS max_rows_to_read = 1000000; -- { serverError TOO_MANY_ROWS }

-- Small offset should still work fine
SELECT count() FROM primes(10, 5) SETTINGS max_rows_to_read = 1000000;
