-- Tags: no-replicated-database, no-asan, no-tsan, no-msan, no-ubsan

SET max_bytes_before_external_group_by = 0;

SET max_memory_usage = '100M';
SELECT cityHash64(rand() % 1000) as n, groupBitmapState(number) FROM numbers_mt(200000000) GROUP BY n FORMAT Null; -- { serverError 241 }
