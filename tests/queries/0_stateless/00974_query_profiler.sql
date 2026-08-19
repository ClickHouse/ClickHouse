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

-- Use one CPU-bound thread here. The longer scan ensures every architecture has enough time to
-- deliver and symbolize samples, while the 10ms period keeps the `trace_log` load bounded when
-- the flaky check runs many copies.
SET max_threads = 1;

-- `Timer::set` accepts periods no shorter than 1ms. A 10ms period makes the profiler's work
-- proportional to this test rather than to the number of concurrently running copies.
SET query_profiler_real_time_period_ns = 1e7;
SET max_rows_to_read = 0;
SET log_queries = 1;
SELECT count(), ignore('test real time query profiler numbers_mt') FROM numbers_mt(1e9);
SET log_queries = 0;
SET query_profiler_real_time_period_ns = 0;
SYSTEM FLUSH LOGS trace_log, query_log;

-- The serverwide profilers (1 second period in the test harness) also produce `trace_log` rows and
-- `QueryProfiler*` counters for this query, so the `trace_log` check below alone cannot prove that
-- the per-query profiler ran. Require a counter threshold that the serverwide profilers cannot
-- reach during the expected duration of this query. The per-query 10ms profiler delivers more
-- than 32 signals or overruns, while the 1-second serverwide profilers have too few opportunities
-- to do so for this single-thread query.
SELECT ProfileEvents['QueryProfilerRuns'] + ProfileEvents['QueryProfilerSignalOverruns'] + ProfileEvents['QueryProfilerConcurrencyOverruns'] > 32
FROM system.query_log
WHERE current_database = currentDatabase() AND query LIKE '%test real time query profiler numbers_mt%' AND query NOT LIKE '%system%' AND type = 'QueryFinish'
ORDER BY event_time DESC LIMIT 1;

-- Check the frames symbolized by `trace_log` itself instead of resolving the addresses again in the
-- verification query. A short period can produce tens of thousands of rows on a slow runner, and
-- calling `addressToLine` for every selected address costs about a millisecond per row in CI. The
-- redundant work made concurrent copies of this test build a backlog that eventually timed out in
-- `SYSTEM FLUSH LOGS`. `symbols` checks the same end-to-end symbolization result without that extra
-- DWARF lookup. The `query_id` and `trace_type` filters keep the oracle specific to this sub-test.
SELECT countIf(hasSubstr(arrayStringConcat(symbols, '\\n'), 'NumbersRangedSource')) > 0
FROM system.trace_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND trace_type = 'Real'
    AND query_id = (SELECT query_id FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%test real time query profiler numbers_mt%' AND query NOT LIKE '%system%' ORDER BY event_time DESC LIMIT 1);

-- Keep the CPU sub-test's counter oracle independent of the 1-second serverwide profilers too.
SET query_profiler_cpu_time_period_ns = 1e7;
SET log_queries = 1;
SET max_rows_to_read = 0;
SELECT count(), ignore('test cpu time query profiler') FROM numbers_mt(1e9);
SET log_queries = 0;
SET query_profiler_cpu_time_period_ns = 0;
SYSTEM FLUSH LOGS trace_log, query_log;

-- Guarded by the same counter threshold as the sub-test above: this proves the per-query 10ms CPU
-- profiler fired during the expected duration of this single-thread query.
SELECT ProfileEvents['QueryProfilerRuns'] + ProfileEvents['QueryProfilerSignalOverruns'] + ProfileEvents['QueryProfilerConcurrencyOverruns'] > 32
FROM system.query_log
WHERE current_database = currentDatabase() AND query LIKE '%test cpu time query profiler%' AND query NOT LIKE '%system%' AND type = 'QueryFinish'
ORDER BY event_time DESC LIMIT 1;

-- Check the stored symbolized frames as above. Filtering to CPU samples prevents a serverwide real
-- time sample from satisfying this oracle.
SELECT countIf(hasSubstr(arrayStringConcat(symbols, '\\n'), 'NumbersRangedSource')) > 0
FROM system.trace_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND trace_type = 'CPU'
    AND query_id = (SELECT query_id FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%test cpu time query profiler%' AND query NOT LIKE '%system%' ORDER BY event_time DESC LIMIT 1);
