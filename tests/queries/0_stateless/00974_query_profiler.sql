-- Tags: no-tsan, no-asan, no-ubsan, no-msan, no-debug, no-fasttest
-- Tag no-fasttest: Not sure why fail even in sequential mode. Disabled for now to make some progress.

SET allow_introspection_functions = 1;
SET trace_profile_events = 0; -- This can inhibit profiler from working, because it prevents sending samples from different profilers concurrently.

SET query_profiler_cpu_time_period_ns = 0;
SET query_profiler_real_time_period_ns = 100000000;
SET log_queries = 1;
SELECT sleep(0.5), ignore('test real time query profiler');
SET log_queries = 0;
SYSTEM FLUSH LOGS trace_log, query_log;

WITH addressToLine(arrayJoin(trace) AS addr) || '#' || demangle(addressToSymbol(addr)) AS symbol
SELECT count() > 0 FROM system.trace_log t WHERE query_id = (SELECT query_id FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%test real time query profiler%' AND query NOT LIKE '%system%' ORDER BY event_time DESC LIMIT 1) AND symbol LIKE '%FunctionSleep%';

SET query_profiler_real_time_period_ns = 0;
SET query_profiler_cpu_time_period_ns = 1000000;
SET log_queries = 1;
SET max_rows_to_read = 0;
SELECT count(), ignore('test cpu time query profiler') FROM numbers_mt(10000000000);
SET log_queries = 0;
SYSTEM FLUSH LOGS trace_log, query_log;

WITH addressToLine(arrayJoin(trace) AS addr) || '#' || demangle(addressToSymbol(addr)) AS symbol
SELECT count() > 0 FROM system.trace_log t WHERE query_id = (SELECT query_id FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%test cpu time query profiler%' AND query NOT LIKE '%system%' ORDER BY event_time DESC LIMIT 1) AND symbol LIKE '%Source%';
