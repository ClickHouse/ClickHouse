SELECT sleep(3.40282e+44); -- { serverError BAD_ARGUMENTS }
SELECT sleep((pow(2, 64) / 1000000) - 1); -- { serverError BAD_ARGUMENTS }
SELECT sleepEachRow(184467440737095516) from numbers(10000); -- { serverError BAD_ARGUMENTS }
SET max_rows_to_read = 0;
SELECT sleepEachRow(pow(2, 31)) from numbers(9007199254740992) settings function_sleep_max_microseconds_per_block = 8589934592000000000; -- { serverError TOO_SLOW }

-- Another corner case, but it requires lots of memory to run (huge block size)
-- SELECT sleepEachRow(pow(2, 31)) from numbers(17179869184) settings max_block_size = 17179869184, function_sleep_max_microseconds_per_block = 8589934592000000000; -- { serverError TOO_SLOW }
