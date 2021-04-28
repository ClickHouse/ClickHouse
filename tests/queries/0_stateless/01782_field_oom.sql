SET max_memory_usage = '500M';
SELECT sumMap([number], [number]) FROM system.numbers_mt; -- { serverError 241 }
