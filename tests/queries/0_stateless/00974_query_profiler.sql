-- Tags: no-debug, no-fasttest, no-llvm-coverage, long
-- Tag no-fasttest: Not sure why fail even in sequential mode. Disabled for now to make some progress.

SET allow_introspection_functions = 1;
SET trace_profile_events = 0; -- This can inhibit profiler from working, because it prevents sending samples from different profilers concurrently.

SET query_profiler_cpu_time_period_ns = 0;
-- Use a period well below the sleep duration, so that a single missed tick (the machine can be
-- heavily loaded, especially under sanitizers) does not leave the query without any sample.
SET query_profiler_real_time_period_ns = 1e7;
SET log_queries = 1;
-- Sleep in 16 threads at once (one single-row block per thread), not in one. Samples are sent to the
-- trace pipe with `WriteBufferFromFileDescriptorDiscardOnFailure`: when a concurrent sample-heavy query
-- (e.g. another instance of this test in the flaky check) keeps the pipe full, samples are silently
-- dropped, and a single sleeping thread produces so few of them that all can be lost. 16 threads give
-- an order of magnitude more chances, and also survive one thread failing to create its profiler timer.
SELECT sum(sleep(0.5)), ignore('test real time query profiler') FROM numbers_mt(16) SETTINGS max_block_size = 1, max_threads = 16;
SET log_queries = 0;
SYSTEM FLUSH LOGS trace_log, query_log;

-- Force enough threads so the query runs well beyond the 100ms profiler period,
-- even when stress tests randomize max_threads to 1.
SET max_threads = 16;
WITH addressToLine(arrayJoin(trace) AS addr) || '#' || demangle(addressToSymbol(addr)) AS symbol
SELECT count() > 0 FROM system.trace_log t WHERE event_date >= yesterday() AND event_time >= now() - 600 AND trace_type = 'Real' AND query_id = (SELECT query_id FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%test real time query profiler%' AND query NOT LIKE '%system%' ORDER BY event_time DESC LIMIT 1) AND symbol LIKE '%FunctionSleep%';

-- Also test the real time profiler with CPU-bound work (numbers_mt).
-- Use a short period so the fast multi-threaded scan reliably spans several
-- profiler periods (a 100ms period races the tens-of-ms scan -> 0 samples).
SET query_profiler_real_time_period_ns = 1e6;
SET max_rows_to_read = 0;
SET log_queries = 1;
SELECT count(), ignore('test real time query profiler numbers_mt') FROM numbers_mt(1e9);
SET log_queries = 0;
SYSTEM FLUSH LOGS trace_log, query_log;

WITH addressToLine(arrayJoin(trace) AS addr) || '#' || demangle(addressToSymbol(addr)) AS symbol
SELECT count() > 0 FROM system.trace_log t WHERE event_date >= yesterday() AND event_time >= now() - 600 AND trace_type = 'Real' AND query_id = (SELECT query_id FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%test real time query profiler numbers_mt%' AND query NOT LIKE '%system%' ORDER BY event_time DESC LIMIT 1) AND symbol LIKE '%Source%';

SET query_profiler_real_time_period_ns = 0;
SET query_profiler_cpu_time_period_ns = 1000000;
SET log_queries = 1;
SET max_rows_to_read = 0;
SELECT count(), ignore('test cpu time query profiler') FROM numbers_mt(1e9);
SET log_queries = 0;
SYSTEM FLUSH LOGS trace_log, query_log;

WITH addressToLine(arrayJoin(trace) AS addr) || '#' || demangle(addressToSymbol(addr)) AS symbol
SELECT count() > 0 FROM system.trace_log t WHERE event_date >= yesterday() AND event_time >= now() - 600 AND trace_type = 'CPU' AND query_id = (SELECT query_id FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%test cpu time query profiler%' AND query NOT LIKE '%system%' ORDER BY event_time DESC LIMIT 1) AND symbol LIKE '%Source%';
