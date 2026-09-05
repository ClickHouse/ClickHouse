-- Tags: no-msan, no-debug, no-fasttest, no-flaky-check, no-llvm-coverage, long
-- Tag no-msan: the sampling query profiler is disabled under Memory Sanitizer (QUERY_PROFILER_SUPPORTED).
-- Tag no-fasttest: Not sure why fail even in sequential mode. Disabled for now to make some progress.
-- Tag no-flaky-check: the flaky check runs dozens of copies of this test concurrently against one
-- server, and everything this test asserts on travels over a single shared, deliberately lossy
-- channel: one trace pipe (which discards writes when full), one `SystemLogQueue`, and one flush
-- thread with a 180 second `waitFlush` deadline that all the concurrent forced flushes serialize
-- on. Under that self-inflicted saturation either every sample of a sub-test can be legitimately
-- dropped (the `trace_log` oracles then read 0) or the final `SYSTEM FLUSH LOGS` times out - both
-- were observed repeatedly even after the test's sample budget was cut to ~600 rows per run. A
-- normal stateless run executes one copy of this test among diverse tests, which the budgeting
-- below handles fine.

SET allow_introspection_functions = 1;
SET trace_profile_events = 0; -- This can inhibit profiler from working, because it prevents sending samples from different profilers concurrently.

-- The `trace_log` pipeline is a shared, bounded resource: every sample from every query goes over
-- one pipe into one `SystemLogQueue`, and one flush thread drains it - on non-sanitizer builds while
-- symbolizing every row through DWARF at about a millisecond per row (`symbolize` in the trace_log
-- server config, which the stateless harness leaves on there). The flaky check runs dozens of copies
-- of this test at once, so the test has to budget the samples it produces: revisions producing about
-- 3800 and about 2300 rows per run both made the flush thread fall behind by more than the 180
-- second `waitFlush` timeout on `amd_binary` (`Timeout exceeded (180 s) while flushing system log
-- 'DB::SystemLogQueue<DB::TraceLogElement>'`), while the last revision that stayed green produced
-- about 1400. Hence below: every profiler period is 10ms and every sub-test lasts about a second,
-- which keeps the whole run under about 600 rows, and there is a single `SYSTEM FLUSH LOGS` at the
-- end instead of one per sub-test.
--
-- Each sub-test also switches the profiler off again as soon as the query under test has finished,
-- so that the `SYSTEM FLUSH LOGS` and the `system.trace_log` verification queries run unprofiled.
-- Profiling them feeds a runaway loop: each sample they take of themselves is another row the next
-- flush has to symbolize, which makes the flush slower, which lets it collect even more samples of
-- itself. In one flaky-check report a single verification query ran for 81 seconds and collected
-- 243778 samples of itself.

SET query_profiler_cpu_time_period_ns = 0;
-- Use a short period: a 100ms period gives only ~5 signals over sleep(0.5), and under a loaded
-- sanitizer server (e.g. the flaky check running this test many times in parallel) a handful of
-- samples can all be lost, leaving 0 rows in `system.trace_log`. 10ms gives ~50 chances.
SET query_profiler_real_time_period_ns = 1e7;
SET log_queries = 1;
-- Sleep in 4 threads at once (one single-row block per thread), not in one, so the sub-test
-- survives a single thread failing to create its profiler timer. This oracle is counter-based (see
-- below), so its samples do not need to survive - 4 threads give about 300 delivered signals
-- against a threshold of about 50 while keeping the `trace_log` byproduct bounded.
-- The marker names the sub-test (`sleep`) so that the oracle below cannot match the `numbers_mt`
-- sub-tests that run after it: with a shared prefix, `ORDER BY event_time DESC LIMIT 1` picked the
-- later CPU-bound query and the sleeping query went unchecked.
SELECT sum(sleep(0.5)), ignore('test real time query profiler sleep') FROM numbers_mt(4) SETTINGS max_block_size = 1, max_threads = 4;
SET log_queries = 0;
SET query_profiler_real_time_period_ns = 0;

-- Use one CPU-bound thread for the two sub-tests below, and bound the duration of the query instead
-- of the amount of work it does: `numbers_mt(1e10)` is more than any supported runner can scan
-- within `max_execution_time`, so with `timeout_overflow_mode = 'break'` the query lasts exactly as
-- long on a fast release build and on a slow sanitizer build alike. That keeps both the
-- number of samples and the oracles below independent of the speed of the machine - a fixed row
-- count made this query last from a fraction of a second to minutes, which is what made earlier
-- fixed thresholds either unreachable on fast runners or reachable by the serverwide profilers
-- alone on slow ones. The result of the query is not printed for the same reason: how many rows it
-- manages to scan is machine-dependent. The two settings are attached to the query rather than to
-- the session, so that the verification queries below are not cut short by the same timeout.
SET max_threads = 1;

-- A 10ms real time period gives about 100 signals per thread over the one second of this query -
-- delivered on wall-clock time, so independent of CPU contention - which is several times the
-- counter oracle's threshold while adding only a hundred or so rows to `trace_log`. One second is
-- enough here for the same reason: the real time timer does not need the query to get any CPU.
SET query_profiler_real_time_period_ns = 1e7;
SET max_rows_to_read = 0;
SET log_queries = 1;
SELECT count(), ignore('test real time query profiler numbers_mt') FROM numbers_mt(1e10) SETTINGS max_execution_time = 1, timeout_overflow_mode = 'break' FORMAT Null;
SET log_queries = 0;
SET query_profiler_real_time_period_ns = 0;

-- The CPU timer fires on CPU time, not wall-clock time, so under a loaded runner the query's single
-- worker thread may get only a fraction of a core and proportionally fewer signals. Use the same
-- 10ms period as above rather than a shorter one - a shorter one does not help, it hurts: with a 2ms
-- period, every signal handler invocation on a sanitizer build costs
-- a noticeable share of the 2ms of CPU time until the next expiration, so `si_overrun` was set on
-- practically every signal and the handler dropped the sample before sending it - the counters kept
-- rising (they count overruns too) while `system.trace_log` received nothing at all for this
-- sub-test, which is exactly how it failed on `amd_asan_ubsan`. A 10ms period also cuts the number
-- of rows this sub-test contributes by five. This query keeps `max_execution_time = 2`, unlike the
-- real time one above: it needs wall-clock time in which to accumulate CPU time when the runner is
-- busy, and it is the sub-test whose oracles need samples to survive.
SET query_profiler_cpu_time_period_ns = 1e7;
SET log_queries = 1;
SET max_rows_to_read = 0;
SELECT count(), ignore('test cpu time query profiler') FROM numbers_mt(1e10) SETTINGS max_execution_time = 2, timeout_overflow_mode = 'break' FORMAT Null;
SET log_queries = 0;
SET query_profiler_cpu_time_period_ns = 0;

-- A single forced flush for all three sub-tests - forced flushes serialize on the shared flush
-- thread across the dozens of concurrent copies of this test in the flaky check, so they are
-- budgeted like the samples themselves.
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
-- profilers can produce for this query instead of against a fixed number. Each of the two serverwide
-- timers expires at most once per second in each thread, plus one extra tick for the randomized first
-- fire, i.e. at most `intDiv(query_duration_ms, 1000) + 1` times; a single expiration bumps these
-- counters by at most two (`QueryProfilerSignalOverruns` by the number of expirations it stands for,
-- and `QueryProfilerRuns` by one), so the serverwide profilers can account for at most
-- `4 * length(thread_ids) * (intDiv(query_duration_ms, 1000) + 1)` here whatever the load. Requiring
-- twice that hard bound therefore cannot be satisfied without the per-query profiler, and is easily
-- met by it: the 10ms timer delivers 100 signals per second in every thread, 50 times the serverwide
-- rate, and it fires on wall-clock time, so this holds however little CPU the query gets.
SELECT ProfileEvents['QueryProfilerRuns'] + ProfileEvents['QueryProfilerSignalOverruns'] + ProfileEvents['QueryProfilerConcurrencyOverruns']
     > 8 * length(thread_ids) * (intDiv(query_duration_ms, 1000) + 1)
FROM system.query_log
WHERE current_database = currentDatabase() AND query LIKE '%test real time query profiler sleep%' AND query NOT LIKE '%system%' AND type = 'QueryFinish'
ORDER BY event_time DESC LIMIT 1;

-- The serverwide profilers also produce `trace_log` rows and `QueryProfiler*` counters for the
-- numbers_mt queries, so the `trace_log` checks below alone cannot prove that the per-query
-- profiler ran. Use the same oracle as for the sleeping query above: require four times the most
-- the two 1 second serverwide timers can contribute for a query of this duration in this many
-- threads.
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
-- thread less than 100ms apart can only come from the per-query 10ms profiler. Unlike a threshold on
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

-- The CPU sub-test deliberately has no counter oracle of its own. Such an oracle has to be compared
-- against what the serverwide profilers can contribute, which is a function of the wall-clock
-- duration of the query, while the CPU timer fires in proportion to the CPU time the query actually
-- receives - on a runner executing dozens of tests at once, an arbitrarily small fraction of it.
-- It would also be redundant: its only merit over the two checks below is that it holds when every
-- sample is lost on the way to `system.trace_log`, and in that case those two checks fail and the
-- test fails anyway. The check that the samples come from the per-query timer rather than from the
-- serverwide one is the interval oracle at the end, which needs only two surviving samples.

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

-- Prove that these samples come from the per-query profiler rather than from the serverwide one, as
-- above: two consecutive samples of the same thread less than 100ms apart can only come from the
-- per-query 2ms profiler.
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
