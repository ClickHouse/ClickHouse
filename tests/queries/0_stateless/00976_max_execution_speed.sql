SET max_execution_speed = 1, max_execution_time = 3, max_rows_to_read = 0;
SELECT count() FROM system.numbers; -- { serverError TIMEOUT_EXCEEDED }
