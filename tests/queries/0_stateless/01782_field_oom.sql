SET max_memory_usage = '1G';
SELECT sumMap([number], [number]) FROM system.numbers_mt; -- { serverError 241 }
