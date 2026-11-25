SELECT sleepEachRow(0.5) FROM numbers(60) SETTINGS max_block_size = 1, function_sleep_max_microseconds_per_block = 0 FORMAT Null;
