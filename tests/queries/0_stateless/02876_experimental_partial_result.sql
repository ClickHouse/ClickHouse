
SET partial_result_update_duration_ms = 10;

SELECT sum(number) FROM numbers_mt(100_000) SETTINGS max_threads = 1; -- { clientError FUNCTION_NOT_ALLOWED }
