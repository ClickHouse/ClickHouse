SET max_memory_usage = 10000000001;

SELECT value FROM system.settings WHERE name = 'max_memory_usage';

SET max_memory_usage = '1G'; -- { serverError 27 }

SELECT value FROM system.settings WHERE name = 'max_memory_usage';
