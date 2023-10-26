-- Tags: no-fasttest

-- Query stops after timeout without an error
SELECT * FROM numbers(100000000) SETTINGS max_block_size=1, max_execution_time=13, timeout_overflow_mode='break' FORMAT Null;

-- Query returns an error when runtime is estimated after 10 sec of execution
SELECT * FROM numbers(100000000) SETTINGS max_block_size=1, max_execution_time=13, timeout_overflow_mode='throw' FORMAT Null; -- { serverError TOO_SLOW }

-- Query returns timeout error before its full execution time is estimated
SELECT * FROM numbers(100000000) SETTINGS max_block_size=1, max_execution_time=2, timeout_overflow_mode='throw' FORMAT Null; -- { serverError TIMEOUT_EXCEEDED }
