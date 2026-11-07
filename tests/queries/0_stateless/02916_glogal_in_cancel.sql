-- The test profile in `tests/config/users.d/limits.yaml` sets `max_rows_to_transfer = 1G`
-- and `max_bytes_to_transfer = 1G`. This test is purely about GLOBAL IN cancellation via
-- `max_execution_time` + `timeout_overflow_mode = 'break'`, so opt out of the transfer
-- limits (set them to 0 = unlimited) to make sure the timeout path is what gets exercised.
set max_execution_time = 0.5, timeout_overflow_mode = 'break', max_rows_to_read = 0,
    max_rows_to_transfer = 0, max_bytes_to_transfer = 0;
SELECT number FROM remote('127.0.0.{1|2|3}', numbers(1)) WHERE number GLOBAL IN (SELECT number FROM numbers(10000000000.)) format Null;
