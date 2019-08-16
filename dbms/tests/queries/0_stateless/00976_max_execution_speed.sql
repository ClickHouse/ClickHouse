SET max_execution_speed = 1, max_execution_time = 3;
SELECT count() FROM system.numbers; -- { serverError 159 }
