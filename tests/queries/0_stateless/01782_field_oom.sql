SET max_memory_usage = '500M', max_execution_time = 0;
SELECT sumMap([number], [number]) FROM system.numbers_mt; -- { serverError MEMORY_LIMIT_EXCEEDED }
