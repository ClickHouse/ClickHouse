-- Tags: no-fasttest

SET max_rows_to_read = 0, max_execution_time = 0, max_estimated_execution_time = 0;

-- Query stops after timeout without an error
SELECT * FROM numbers(100000000) SETTINGS max_block_size=1, max_execution_time=2, timeout_overflow_mode='break' FORMAT Null;

-- Query returns an error when runtime is estimated after timeout_before_checking_execution_speed passed
SELECT * FROM numbers(100000000) SETTINGS max_block_size=1, timeout_before_checking_execution_speed=1, max_estimated_execution_time=2, timeout_overflow_mode='throw' FORMAT Null; -- { serverError TOO_SLOW }

-- Query returns timeout error before its full execution time is estimated
SELECT * FROM numbers(100000000) SETTINGS max_block_size=1, timeout_before_checking_execution_speed=1, max_execution_time=2, timeout_overflow_mode='throw' FORMAT Null; -- { serverError TIMEOUT_EXCEEDED }
