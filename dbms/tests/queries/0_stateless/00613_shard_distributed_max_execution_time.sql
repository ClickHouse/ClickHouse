SET max_execution_time = 1, timeout_overflow_mode = 'break';
SELECT DISTINCT * FROM remote('127.0.0.{2,3}', system.numbers) WHERE number < 10;
