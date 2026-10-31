-- Tags: no-fasttest, long
-- no-fasttest: needs enough rows of moderate SQL text that an unpatched parse loop overshoots the limit.
-- long: `fuzzQuery` holds one process-global mutex per row, so copies of this test run by the flaky
-- check serialize against each other and its two `fuzzQuery` cases alone cost about 110 s of a run.

-- The query-parsing functions parse one row at a time inside a single pipeline task. Cancellation is
-- only polled between tasks, so before the fix a whole block ran to completion and the query outlived
-- `max_execution_time` and `KILL QUERY` (measured: 23 s under a 1 s limit; 1247 s in CI under MSan).
--
-- Note the assertion is on the ELAPSED TIME, not on the error: an unpatched server also raises
-- TIMEOUT_EXCEEDED, just tens of seconds late, so `-- { serverError TIMEOUT_EXCEEDED }` alone would
-- pass without the fix. Case 7 is the load-bearing one and checks how late each case stopped.
--
-- Every timed query pins `max_block_size` and `max_threads`, which the test runner otherwise
-- randomizes: a small block lets an unpatched server finish its uninterruptible unit soon enough to
-- stay under case 7's threshold, silently disarming it. The pins are per query so that they cannot
-- leak into case 7's own read. Each timed query carries a `04691_` marker that case 7 counts.

-- `max_execution_time` runs from query start, so it also has to cover parsing, analysis, planning and
-- pipeline construction. On a sanitizer build that prelude alone reached 276 ms of a 300 ms budget, and
-- the query then timed out before reading a row, leaving the row loop below jointly unexercised. The
-- limit therefore has to exceed the prelude by enough that what remains still reaches the loop; 3 s
-- sits between the observed 1.6 s worst-case prelude and the tens of seconds an unpolled block costs.
SET max_execution_time = 3;
SET allow_fuzz_query_functions = 1;

-- Every row below carries a long trailing comment. The poll is throttled on accumulated input BYTES,
-- while a row's parse cost is set by how many AST nodes it has, so the longest a cancellation can go
-- unobserved is one stride of parsing: 64 KiB divided by the row size, times the per-row cost. Rows of
-- 40 `OR` clauses in 458 bytes are the worst case for that ratio, and on MSan one stride cost 29 s
-- against this 3 s limit. Comment bytes count toward the stride but add no AST, so padding a row to
-- 2462 bytes cuts the wait more than fivefold for no extra parse work. The row counts are halved to
-- hold peak memory where it was, and both have to stay large enough that one unpolled block still
-- exceeds case 7's threshold by a wide margin.

-- 1. formatQuery: many rows of moderate SQL text in one block.
SELECT sum(length(formatQuery('SELECT ' || toString(number) || ' WHERE x=0' || repeat(' OR (y = 1)', 40) || ' -- ' || repeat('c', 2000)))) /* 04691_timed */
FROM numbers(100000) FORMAT Null SETTINGS max_block_size = 200000, max_threads = 1; -- { serverError TIMEOUT_EXCEEDED }

-- 2. formatQueryOrNull must raise, not swallow the cancellation into NULLs. Its per-row `catch (...)`
--    turns any exception into NULL and continues, so the check has to sit outside that handler.
SELECT sum(length(formatQueryOrNull('SELECT ' || toString(number) || ' WHERE x=0' || repeat(' OR (y = 1)', 40) || ' -- ' || repeat('c', 2000)))) /* 04691_timed */
FROM numbers(100000) FORMAT Null SETTINGS max_block_size = 200000, max_threads = 1; -- { serverError TIMEOUT_EXCEEDED }

-- 3. The single-line variants share the same loop.
SELECT sum(length(formatQuerySingleLine('SELECT ' || toString(number) || ' WHERE x=0' || repeat(' OR (y = 1)', 40) || ' -- ' || repeat('c', 2000)))) /* 04691_timed */
FROM numbers(100000) FORMAT Null SETTINGS max_block_size = 200000, max_threads = 1; -- { serverError TIMEOUT_EXCEEDED }

SELECT sum(length(formatQuerySingleLineOrNull('SELECT ' || toString(number) || ' WHERE x=0' || repeat(' OR (y = 1)', 40) || ' -- ' || repeat('c', 2000)))) /* 04691_timed */
FROM numbers(100000) FORMAT Null SETTINGS max_block_size = 200000, max_threads = 1; -- { serverError TIMEOUT_EXCEEDED }

-- 4. Siblings with the same per-row parse loop.
SELECT sum(length(parseQueryToJSON('SELECT ' || toString(number) || ' WHERE x=0' || repeat(' OR (y = 1)', 40) || ' -- ' || repeat('c', 2000)))) /* 04691_timed */
FROM numbers(100000) FORMAT Null SETTINGS max_block_size = 200000, max_threads = 1; -- { serverError TIMEOUT_EXCEEDED }

--    `highlightQuery` and `tokenizeQuery` share one row loop in FunctionQueryTokenization.
--    `highlightQuery` runs a full parse per row and covers that loop. `tokenizeQuery` is lexer-only and
--    its cost is dominated by the size of the token array it builds, so it exhausts memory well before
--    it overshoots a time limit; it gets no case of its own rather than one whose oracle cannot redden.
SELECT sum(length(highlightQuery('SELECT ' || toString(number) || ' WHERE x=0' || repeat(' OR (y = 1)', 40) || ' -- ' || repeat('c', 2000)))) /* 04691_timed */
FROM numbers(60000) FORMAT Null SETTINGS max_block_size = 200000, max_threads = 1; -- { serverError TIMEOUT_EXCEEDED }

-- 5. `fuzzQuery` has one row loop per argument shape and both need the poll.
--    Non-constant argument, so this one takes the ColumnString loop.
SELECT sum(length(fuzzQuery('SELECT ' || toString(number) || ' WHERE x=0' || repeat(' OR (y = 1)', 40) || ' -- ' || repeat('c', 2000)))) /* 04691_timed */
FROM numbers(100000) FORMAT Null SETTINGS max_block_size = 200000, max_threads = 1; -- { serverError TIMEOUT_EXCEEDED }

--    Constant argument: `fuzzQuery` opts out of the default constant handling, so this reaches the
--    separate ColumnConst loop. It is not folded away because the function is non-deterministic.
SELECT sum(length(fuzzQuery('SELECT 1 WHERE x=0' || repeat(' OR (y = 1)', 40) || ' -- ' || repeat('c', 2000)))) /* 04691_timed */
FROM numbers(100000) FORMAT Null SETTINGS max_block_size = 200000, max_threads = 1; -- { serverError TIMEOUT_EXCEEDED }

--    `formatQueryFromJSON` takes JSON from a scalar computed once and then materialized. Nesting the
--    `parseQueryToJSON` call inside the timed expression instead would let the inner function's own
--    poll raise first, leaving this one unexercised.
WITH parseQueryToJSON('SELECT 1 WHERE x=0' || repeat(' OR (y = 1)', 40)) AS json
SELECT sum(length(formatQueryFromJSON(materialize(json)))) /* 04691_timed */
FROM numbers(100000) FORMAT Null SETTINGS max_block_size = 200000, max_threads = 1; -- { serverError TIMEOUT_EXCEEDED }

-- 6. `timeout_overflow_mode = 'break'` needs its own case. There `checkTimeLimit` returns false
--    instead of throwing, and this fix turns that false into a hard stop, so the observable behaviour
--    changes from "ran the block out, returned a result" to "stopped mid-block". The `throw`-mode
--    cases above cannot exercise that return at all, and `timeout_overflow_mode` is not one of the
--    settings the test runner randomizes, so nothing else would ever reach it. `04648` covers the
--    identical decision for `geohashesInBox` the same way.
--    A stopped aggregate emits no row rather than a partial sum, and this query succeeds in both
--    directions, so the latency bound in case 7 is what discriminates: 34413 ms unpolled, 3005 ms with
--    the poll. The marker is separate because a successful query lands in `query_log` as `QueryFinish`.
SELECT sum(length(formatQuery('SELECT ' || toString(number) || ' WHERE x=0' || repeat(' OR (y = 1)', 40) || ' -- ' || repeat('c', 2000)))) /* 04691_break */
FROM numbers(100000) FORMAT Null
SETTINGS max_block_size = 200000, max_threads = 1, timeout_overflow_mode = 'break';

-- 7. The real assertion: every case above stopped near its limit rather than running its block out.
--    The bound is on CPU time, not on `query_duration_ms`: wall clock also absorbs scheduling delay,
--    and this test runs concurrently with copies of itself, so a wall-clock budget wide enough for a
--    starved host would have to exceed the pre-fix cost it exists to detect. Unpolled a single one of
--    these statements burns about 35 s of CPU; with the poll it stops within a stride of its limit.
--    `count() = 10` keeps the assertion from going vacuous: countIf(...) = 0 over an empty result set is
--    trivially true, so a marker that stopped matching would silently disarm the whole test.
--    `type != 'QueryStart'` rather than `= 'ExceptionWhileProcessing'` because the break case above
--    succeeds; `04648`'s duration check filters the same way. Both markers share the `04691_` prefix so
--    one pattern counts all ten, and each query carries exactly one marker.
--    max_execution_time = 0: the session limit above applies to the flush and to this unindexed
--    query_log scan as well, and on a loaded sanitizer host either can exceed one second and time the
--    oracle itself out. SYSTEM FLUSH LOGS takes no SETTINGS clause, hence the session-level SET.
--    enable_parallel_replicas = 0: the test runner can inject parallel replicas, which this local
--    system-table read must not go through.
SET max_execution_time = 0;
SYSTEM FLUSH LOGS query_log;

--    `countIf(read_rows = 0) = 0` is what keeps the CPU bound from passing vacuously: a query that
--    exhausted the limit during planning never reached the row loop, so it trivially satisfies a CPU
--    bound while asserting nothing about cancellation. Observed on a sanitizer build, where 16 of 19
--    runs read no rows at all and the whole test still reported success.
SELECT 'all bounded',
       count() = 10
   AND countIf(read_rows = 0) = 0
   AND countIf(ProfileEvents['UserTimeMicroseconds'] + ProfileEvents['SystemTimeMicroseconds'] > 15000000) = 0
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type != 'QueryStart'
  AND event_time > now() - INTERVAL 10 MINUTE
  AND (query LIKE '%04691\_timed%' OR query LIKE '%04691\_break%')
SETTINGS max_execution_time = 0, enable_parallel_replicas = 0;

-- 8. Results are unchanged when no limit is hit, including the OrNull NULL-on-parse-error contract.
SELECT formatQuery('select    a,b from  t') SETTINGS max_execution_time = 0;
SELECT formatQuerySingleLine('select    a,b from  t') SETTINGS max_execution_time = 0;
SELECT formatQueryOrNull('this is not a query') IS NULL SETTINGS max_execution_time = 0;

-- 9. The liveness arm: a regression that made the check fire when it should not, or throw
--    unconditionally, has to be caught somewhere, and everywhere else these functions appear
--    TIMEOUT_EXCEEDED is the expected result. One call per polled row loop, no limit set.
--    Each call has to actually REACH the check, and the check is throttled on accumulated input
--    bytes, so a single short query never polls at all and would make this arm vacuous. The stride
--    is charged in bytes while the parse costs per row, so a few long rows reach it far more cheaply
--    than many short ones: 100 rows of 7809 bytes cross the 64 KiB stride 11 times over.
--    The padding sits in a string literal rather than in a trailing comment because a comment is not
--    part of the AST: `parseQueryToJSON` emits the same 339 bytes however long the comment is, so the
--    two `formatQueryFromJSON` calls below, whose rows are that JSON, would never cross the stride.
--    `fuzzQuery` is non-deterministic, so only the length is asserted.
SELECT sum(length(formatQuery(materialize('SELECT ''' || repeat('x', 7800) || '''')))) > 0 FROM numbers(100)
SETTINGS max_execution_time = 0, max_block_size = 200000;
SELECT sum(length(parseQueryToJSON(materialize('SELECT ''' || repeat('x', 7800) || '''')))) > 0 FROM numbers(100)
SETTINGS max_execution_time = 0, max_block_size = 200000;
SELECT sum(length(highlightQuery(materialize('SELECT ''' || repeat('x', 7800) || '''')))) > 0 FROM numbers(100)
SETTINGS max_execution_time = 0, max_block_size = 200000;
SELECT sum(length(tokenizeQuery(materialize('SELECT ''' || repeat('x', 7800) || '''')))) > 0 FROM numbers(100)
SETTINGS max_execution_time = 0, max_block_size = 200000;
SELECT sum(length(fuzzQuery(materialize('SELECT ''' || repeat('x', 7800) || '''')))) > 0 FROM numbers(100)
SETTINGS max_execution_time = 0, max_block_size = 200000;
WITH parseQueryToJSON('SELECT ''' || repeat('x', 7800) || '''') AS json
SELECT sum(length(formatQueryFromJSON(materialize(json)))) > 0 FROM numbers(100)
SETTINGS max_execution_time = 0, max_block_size = 200000;

--    The two-argument form takes a separate branch, so without a line of its own nothing would redden
--    if the check threw unconditionally there.
WITH parseQueryToJSON('SELECT ''' || repeat('x', 7800) || '''') AS json
SELECT sum(length(formatQueryFromJSON(materialize(json), materialize('SELECT ''' || repeat('x', 7800) || '''')))) > 0
FROM numbers(100) SETTINGS max_execution_time = 0, max_block_size = 200000;

-- 10. The poll must read the EXECUTING query, not the one that built the function. `ALTER` rebuilds the
--     partition key from its own query context and `adjustPartitionKey` hands that same object to later
--     inserts, rebuilding only when the key contains `modulo`, hence the plain key here. Reading a
--     per-instance `QueryStatus` would fail this insert on the ALTER's expired limit, so the ALTER's
--     limit has to be shorter than the wait below: with a limit longer than the wait, a stale timer has
--     not expired by the time the insert runs and the case passes either way.
--     `break` is what lets the limit be that short without the ALTER timing itself out: on expiry
--     `CancellationChecker::cancelTask` calls `checkTimeLimit` instead of `cancelQuery(TIMEOUT)`, so
--     `is_killed` is never set and the ALTER always completes, while the stopwatch a stale status would
--     read keeps running. The insert only needs one stride crossing for the poll to fire, and padded
--     rows reach it with few enough rows to keep the partition count low.
DROP TABLE IF EXISTS t_04691_retained;
CREATE TABLE t_04691_retained (q String, n UInt64)
ENGINE = MergeTree PARTITION BY cityHash64(formatQuery(q)) ORDER BY n;
ALTER TABLE t_04691_retained ADD COLUMN extra UInt8 DEFAULT 0
SETTINGS max_execution_time = 0.5, timeout_overflow_mode = 'break';
SELECT sleep(1) FORMAT Null SETTINGS max_execution_time = 0;
INSERT INTO t_04691_retained (q, n)
SELECT 'SELECT ' || toString(number % 4) || ' -- ' || repeat('x', 700), number FROM numbers(200)
SETTINGS max_execution_time = 0, max_block_size = 200000;
SELECT count() FROM t_04691_retained SETTINGS max_execution_time = 0;
DROP TABLE t_04691_retained;

SELECT 'ok';
