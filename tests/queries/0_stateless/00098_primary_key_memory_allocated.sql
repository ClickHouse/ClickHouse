-- Tags: stateful, no-object-storage
-- Force PK load
SELECT CounterID FROM test.hits WHERE CounterID > 0 LIMIT 1 FORMAT Null;
-- Check PK size
SELECT primary_key_bytes_in_memory > 0, primary_key_bytes_in_memory < 16000, primary_key_bytes_in_memory_allocated < 16000, primary_key_bytes_in_memory_allocated / primary_key_bytes_in_memory < 1.1 FROM system.parts WHERE database = 'test' AND table = 'hits';
