SET max_memory_usage = '1G';
SELECT range(65535) FROM system.one ARRAY JOIN range(65536) AS number; -- { serverError MEMORY_LIMIT_EXCEEDED }
