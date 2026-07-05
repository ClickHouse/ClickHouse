-- Tags: no-replicated-database, no-asan, no-tsan, no-msan, no-ubsan, no-coverage
-- Sanitizers have their own mechanism of tracking the allocation/deallocations and it doesn't work with our MemoryTracker. 

SET max_bytes_before_external_group_by = 0;
SET max_bytes_ratio_before_external_group_by = 0;

SET max_memory_usage = '100M', max_rows_to_read = '1G';
SELECT cityHash64(rand() % 1000) as n, groupBitmapState(number) FROM numbers_mt(200000000) GROUP BY n FORMAT Null; -- { serverError MEMORY_LIMIT_EXCEEDED }
