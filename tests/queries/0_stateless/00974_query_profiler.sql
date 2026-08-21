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
-- These counters are shared with the serverwide profilers, which the stateless test harness enables
-- with a 1 second period (`serverwide_trace_collector.xml`), so compare the count against what those
-- profilers can produce for this query instead of against a fixed number: each of the two serverwide
-- timers fires at most once per second in each thread, plus one extra tick for the randomized first
-- fire, i.e. at most `2 * length(thread_ids) * (duration in seconds + 1)`. Requiring four times that
-- bound cannot be satisfied by the serverwide profilers however fast or slow the runner is, while the
-- 10ms per-query profiler delivers 100 signals per second in every thread - 50 times the serverwide
-- rate - so the check keeps a wide margin on both.
SELECT ProfileEvents['QueryProfilerRuns'] + ProfileEvents['QueryProfilerSignalOverruns'] + ProfileEvents['QueryProfilerConcurrencyOverruns']
     > 8 * length(thread_ids) * (intDiv(query_duration_ms, 1000) + 1)
FROM system.query_log
WHERE current_database = currentDatabase() AND query LIKE '%test real time query profiler%' AND query NOT LIKE '%system%' AND type = 'QueryFinish'
ORDER BY event_time DESC LIMIT 1;

-- Use one CPU-bound thread here, and bound the duration of the query instead of the amount of work
-- it does: `numbers_mt(1e10)` is more than any supported runner can scan within `max_execution_time`,
-- so with `timeout_overflow_mode = 'break'` the query runs for about three seconds on a fast release
-- build and on a slow sanitizer build alike. That keeps both the number of samples and the oracles
-- below independent of the speed of the machine - a fixed row count made this query last from a
-- fraction of a second to minutes, which is what made the previous thresholds either unreachable on
-- fast runners or reachable by the serverwide profilers alone on slow ones. The result of the query
-- is not printed for the same reason: how many rows it manages to scan is machine-dependent. The two
-- settings are attached to the query rather than to the session, so that the verification queries
-- below are not cut short by the same timeout.
SET max_threads = 1;

-- `Timer::set` accepts periods no shorter than 1ms. A 2ms period gives about 1500 samples per thread
-- over the three seconds of this query, which is plenty for the oracles below and keeps the
-- `trace_log` load bounded when the flaky check runs many copies of this test at once.
SET query_profiler_real_time_period_ns = 2e6;
SET max_rows_to_read = 0;
SET log_queries = 1;
SELECT count(), ignore('test real time query profiler numbers_mt') FROM numbers_mt(1e10) SETTINGS max_execution_time = 3, timeout_overflow_mode = 'break' FORMAT Null;
SET log_queries = 0;
SET query_profiler_real_time_period_ns = 0;
SYSTEM FLUSH LOGS trace_log, query_log;

-- The serverwide profilers (1 second period in the test harness) also produce `trace_log` rows and
-- `QueryProfiler*` counters for this query, so the `trace_log` check below alone cannot prove that
-- the per-query profiler ran. Use the same oracle as for the sleeping query above: require four
-- times the most the two 1 second serverwide timers can contribute for a query of this duration in
-- this many threads. The 2ms per-query profiler produces 500 signals per second in every thread,
-- 250 times the serverwide rate.
SELECT ProfileEvents['QueryProfilerRuns'] + ProfileEvents['QueryProfilerSignalOverruns'] + ProfileEvents['QueryProfilerConcurrencyOverruns']
     > 8 * length(thread_ids) * (intDiv(query_duration_ms, 1000) + 1)
FROM system.query_log
WHERE current_database = currentDatabase() AND query LIKE '%test real time query profiler numbers_mt%' AND query NOT LIKE '%system%' AND type = 'QueryFinish'
ORDER BY event_time DESC LIMIT 1;

-- Symbolize a bounded sample of the rows instead of all of them: symbolizing every sample of this
-- query made this verification query read 94887 rows in 318 seconds in the flaky check until it was
-- killed with `Estimated query execution time (1385.003 seconds) is too long. Maximum: 600`
-- (`TOO_SLOW`). 1000 rows keep the work bounded without weakening the check: most of this query's
-- samples carry a `Source` frame, and when fewer than 1000 samples were delivered the `LIMIT` takes
-- all of them. The `query_id` filter sits inside the `LIMIT` subquery, so other queries' samples are
-- never symbolized either, and the `trace_type` filter keeps the oracle specific to this sub-test.
-- Resolve the symbols here rather than reading the `symbols` column of `system.trace_log`: the
-- stateless harness turns in-flush symbolization off on sanitizer builds
-- (`trace_log_no_symbolize.xml`), so that column is empty there. Only `addressToSymbol` is used -
-- `addressToLine` walks DWARF and costs about a millisecond per row in CI.
SELECT countIf(arrayExists(addr -> demangle(addressToSymbol(addr)) LIKE '%Source%', trace)) > 0
FROM
(
    SELECT trace
    FROM system.trace_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600
        AND trace_type = 'Real'
        AND query_id = (SELECT query_id FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%test real time query profiler numbers_mt%' AND query NOT LIKE '%system%' ORDER BY event_time DESC LIMIT 1)
    LIMIT 1000
);

-- Prove that these samples come from the per-query profiler rather than from the serverwide one: a
-- serverwide timer fires at most once per second in a thread, so two consecutive samples of the same
-- thread less than 100ms apart can only come from the per-query 2ms profiler. Unlike a threshold on
-- the number of rows, this needs just two surviving samples of one thread, so it holds even when the
-- lossy trace channel (concurrency cap, timer overruns, full trace pipe) discards most of them.
SELECT countIf(gap < 100000000) > 0
FROM
(
    SELECT arrayJoin(arrayFilter(x -> x > 0, arrayDifference(arraySort(groupArray(timestamp_ns))))) AS gap
    FROM system.trace_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600
        AND trace_type = 'Real'
        AND query_id = (SELECT query_id FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%test real time query profiler numbers_mt%' AND query NOT LIKE '%system%' ORDER BY event_time DESC LIMIT 1)
    GROUP BY thread_id
);

-- The CPU time profiler is checked the same way, on a query bounded the same way.
SET query_profiler_cpu_time_period_ns = 2e6;
SET log_queries = 1;
SET max_rows_to_read = 0;
SELECT count(), ignore('test cpu time query profiler') FROM numbers_mt(1e10) SETTINGS max_execution_time = 3, timeout_overflow_mode = 'break' FORMAT Null;
SET log_queries = 0;
SET query_profiler_cpu_time_period_ns = 0;
SYSTEM FLUSH LOGS trace_log, query_log;

-- Guarded by the same oracle as the sub-test above. The CPU timer fires on CPU time, so it delivers
-- fewer signals than the real time one when the runner is loaded, but the serverwide bound is
-- computed from the wall-clock duration and is an upper bound for the CPU timer as well.
SELECT ProfileEvents['QueryProfilerRuns'] + ProfileEvents['QueryProfilerSignalOverruns'] + ProfileEvents['QueryProfilerConcurrencyOverruns']
     > 8 * length(thread_ids) * (intDiv(query_duration_ms, 1000) + 1)
FROM system.query_log
WHERE current_database = currentDatabase() AND query LIKE '%test cpu time query profiler%' AND query NOT LIKE '%system%' AND type = 'QueryFinish'
ORDER BY event_time DESC LIMIT 1;

-- Bounded and symbolized as above. Filtering to CPU samples prevents a serverwide real time sample
-- from satisfying this oracle.
SELECT countIf(arrayExists(addr -> demangle(addressToSymbol(addr)) LIKE '%Source%', trace)) > 0
FROM
(
    SELECT trace
    FROM system.trace_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600
        AND trace_type = 'CPU'
        AND query_id = (SELECT query_id FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%test cpu time query profiler%' AND query NOT LIKE '%system%' ORDER BY event_time DESC LIMIT 1)
    LIMIT 1000
);

-- Prove that these samples come from the per-query profiler rather than from the serverwide one: a
-- serverwide timer fires at most once per second in a thread, so two consecutive samples of the same
-- thread less than 100ms apart can only come from the per-query 2ms profiler. Unlike a threshold on
-- the number of rows, this needs just two surviving samples of one thread, so it holds even when the
-- lossy trace channel (concurrency cap, timer overruns, full trace pipe) discards most of them.
SELECT countIf(gap < 100000000) > 0
FROM
(
    SELECT arrayJoin(arrayFilter(x -> x > 0, arrayDifference(arraySort(groupArray(timestamp_ns))))) AS gap
    FROM system.trace_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600
        AND trace_type = 'CPU'
        AND query_id = (SELECT query_id FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%test cpu time query profiler%' AND query NOT LIKE '%system%' ORDER BY event_time DESC LIMIT 1)
    GROUP BY thread_id
);
