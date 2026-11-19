set max_execution_time = 0.5, timeout_overflow_mode = 'break', max_rows_to_read = 0;
SELECT number FROM remote('127.0.0.{1|2|3}', numbers(1)) WHERE number GLOBAL IN (SELECT number FROM numbers(10000000000.)) format Null;
