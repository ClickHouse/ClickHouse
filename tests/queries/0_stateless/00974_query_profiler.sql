-- Tags: no-msan, no-debug, no-fasttest, no-llvm-coverage, long
-- Tag no-msan: the sampling query profiler is disabled under Memory Sanitizer (QUERY_PROFILER_SUPPORTED).
-- Tag no-fasttest: Not sure why fail even in sequential mode. Disabled for now to make some progress.

SET allow_introspection_functions = 1;
SET trace_profile_events = 0; -- This can inhibit profiler from working, because it prevents sending samples from different profilers concurrently.

-- Each sub-test below switches the profiler off again as soon as the query under test has finished,
-- so that the following `SYSTEM FLUSH LOGS` and `system.trace_log` verification queries run
-- unprofiled. Profiling them feeds a runaway loop: `TraceLogElement::appendToBlock` symbolizes every
-- frame of every sample through DWARF while the flush thread holds up the queue (about 1 ms per row
-- in CI), so each sample these queries take of themselves is another row the next flush has to
-- symbolize, which makes the flush slower, which lets it collect even more samples of itself. It
-- ends in `Timeout exceeded (180 s) while flushing system log
-- 'DB::SystemLogQueue<DB::TraceLogElement>'` once the flaky check runs this test many times in
-- parallel: in the report below a single verification query ran for 81 seconds and collected 243778
-- samples of itself, one `SYSTEM FLUSH LOGS` collected 96283, and those two queries alone accounted
-- for 81% of all the query-attributed samples in the job - while the queries actually under test
-- contributed about 27000 each.

SET query_profiler_cpu_time_period_ns = 0;
-- Use a short period: a 100ms period gives only ~5 signals over sleep(0.5), and under a loaded
-- sanitizer server (e.g. the flaky check running this test many times in parallel) a handful of
-- samples can all be lost, leaving 0 rows in `system.trace_log`. 10ms gives ~50 chances.
SET query_profiler_real_time_period_ns = 1e7;
SET log_queries = 1;
-- Sleep in 16 threads at once (one single-row block per thread), not in one, so the sub-test
-- survives a single thread failing to create its profiler timer.
SELECT sum(sleep(0.5)), ignore('test real time query profiler') FROM numbers_mt(16) SETTINGS max_block_size = 1, max_threads = 16;
SET log_queries = 0;
SET query_profiler_real_time_period_ns = 0;
SYSTEM FLUSH LOGS trace_log, query_log;

-- Do not check `system.trace_log` for the sleeping query: samples travel over a lossy channel (the
-- signal handler drops them beyond 100 concurrent invocations and on timer overruns, and the trace
-- pipe silently discards writes when full), so under a loaded server (e.g. the flaky check running
-- this test many times in parallel) all samples of a short mostly-idle query can legitimately be
-- lost. The ProfileEvents counters below are incremented in the signal handler on every delivered
-- profiler signal - including those whose samples are dropped later - so they prove the real time
-- profiler fires for an off-CPU (sleeping) query without depending on trace delivery. End-to-end
-- delivery to `system.trace_log` and symbolization are checked by the CPU-bound sub-tests below.
-- These counters are shared with the serverwide profiler, which the stateless test harness enables
-- with a 1 second period (`serverwide_trace_collector.xml`), so require a count the serverwide
-- profiler cannot reach: it contributes at most ~1 real time tick per thread over the 0.5 second
-- query (and no CPU ticks, as the threads sleep), i.e. about 20 with service threads, while the
-- 10ms per-query profiler is expected to deliver ~50 signals in each of the 16 threads (~800).
SELECT ProfileEvents['QueryProfilerRuns'] + ProfileEvents['QueryProfilerSignalOverruns'] + ProfileEvents['QueryProfilerConcurrencyOverruns'] > 64
FROM system.query_log
WHERE current_database = currentDatabase() AND query LIKE '%test real time query profiler%' AND query NOT LIKE '%system%' AND type = 'QueryFinish'
ORDER BY event_time DESC LIMIT 1;

-- Force enough threads so the query runs well beyond the profiler period,
-- even when stress tests randomize max_threads to 1.
SET max_threads = 16;

-- Also test the real time profiler with CPU-bound work (numbers_mt).
-- Use a short period so the fast multi-threaded scan reliably spans several
-- profiler periods (a 100ms period races the tens-of-ms scan -> 0 samples).
SET query_profiler_real_time_period_ns = 1e6;
SET max_rows_to_read = 0;
SET log_queries = 1;
SELECT count(), ignore('test real time query profiler numbers_mt') FROM numbers_mt(1e9);
SET log_queries = 0;
SET query_profiler_real_time_period_ns = 0;
SYSTEM FLUSH LOGS trace_log, query_log;

WITH addressToLine(arrayJoin(trace) AS addr) || '#' || demangle(addressToSymbol(addr)) AS symbol
SELECT count() > 0 FROM system.trace_log t WHERE event_date >= yesterday() AND event_time >= now() - 600 AND query_id = (SELECT query_id FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%test real time query profiler numbers_mt%' AND query NOT LIKE '%system%' ORDER BY event_time DESC LIMIT 1) AND symbol LIKE '%Source%';

SET query_profiler_cpu_time_period_ns = 1000000;
SET log_queries = 1;
SET max_rows_to_read = 0;
SELECT count(), ignore('test cpu time query profiler') FROM numbers_mt(1e9);
SET log_queries = 0;
SET query_profiler_cpu_time_period_ns = 0;
SYSTEM FLUSH LOGS trace_log, query_log;

WITH addressToLine(arrayJoin(trace) AS addr) || '#' || demangle(addressToSymbol(addr)) AS symbol
SELECT count() > 0 FROM system.trace_log t WHERE event_date >= yesterday() AND event_time >= now() - 600 AND query_id = (SELECT query_id FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%test cpu time query profiler%' AND query NOT LIKE '%system%' ORDER BY event_time DESC LIMIT 1) AND symbol LIKE '%Source%';
