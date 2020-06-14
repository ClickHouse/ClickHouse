SET max_memory_usage = 300000000;
SET max_bytes_before_external_sort = 20000000;
SELECT number FROM (SELECT number FROM system.numbers LIMIT 10000000) ORDER BY number * 1234567890123456789 LIMIT 9999990, 10;
