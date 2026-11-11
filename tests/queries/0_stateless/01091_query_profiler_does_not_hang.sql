-- Tags: no-msan, no-debug, long
-- Tag no-msan: the sampling query profiler is disabled under Memory Sanitizer (QUERY_PROFILER_SUPPORTED),
-- so the signal storm this test guards against is never produced.

SET query_profiler_cpu_time_period_ns = 1, max_rows_to_read = 0;
SELECT count() FROM numbers_mt(1000000000);
