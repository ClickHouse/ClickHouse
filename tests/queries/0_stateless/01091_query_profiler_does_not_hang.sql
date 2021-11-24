-- Tags: no-tsan, no-asan, no-ubsan, no-msan, no-debug, no-unbundled

SET query_profiler_cpu_time_period_ns = 1;
SELECT count() FROM numbers_mt(1000000000);
