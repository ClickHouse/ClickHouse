-- Tags: no-replicated-database

SET max_memory_usage = '100M';
SELECT cityHash64(rand() % 1000) as n, groupBitmapState(number) FROM numbers_mt(2000000000) GROUP BY n FORMAT Null; -- { serverError 241 }
