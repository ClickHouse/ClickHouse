SELECT sleep(3.40282e+44); -- { serverError BAD_ARGUMENTS }
SELECT sleep((pow(2, 64) / 1000000) - 1); -- { serverError BAD_ARGUMENTS }
SELECT sleepEachRow(184467440737095516) from numbers(10000); -- { serverError BAD_ARGUMENTS }
SELECT sleepEachRow(pow(2, 31)) from numbers(9007199254740992) settings function_sleep_max_microseconds_per_block = 8589934592000000000; -- { serverError TOO_SLOW }
