-- Tags: no-tsan, no-asan, no-ubsan, no-msan, no-debug

SET query_profiler_cpu_time_period_ns = 1, max_rows_to_read = 0;
SELECT count() FROM numbers_mt(1000000000);
