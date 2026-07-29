-- Tags: no-asan, no-tsan, no-msan, no-ubsan, no-sanitize-coverage, no-llvm-coverage, no-darwin
-- no-darwin: source-line symbolization needs DWARF, which on Mach-O lives in a separate .dSYM bundle
-- that the standard macOS build does not produce (ELF embeds DWARF directly in the binary).

SET log_queries = 1;
SET log_query_threads = 1;
SET query_profiler_real_time_period_ns = 100000000;
SELECT sleep(1);
SYSTEM FLUSH LOGS trace_log;

SELECT countIf(arrayExists(x -> x LIKE '%:%:%', lines)) > 1 FROM system.trace_log WHERE event_date >= yesterday() AND event_time >= now() - 600;
