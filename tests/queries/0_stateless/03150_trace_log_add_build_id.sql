-- Tags: no-asan, no-tsan, no-msan, no-ubsan, no-sanitize-coverage

SET log_queries = 1;
SET log_query_threads = 1;
SET query_profiler_real_time_period_ns = 100000000;
SELECT sleep(1);
SYSTEM FLUSH LOGS;

SELECT COUNT(*) > 1 FROM system.trace_log WHERE build_id IS NOT NULL;

