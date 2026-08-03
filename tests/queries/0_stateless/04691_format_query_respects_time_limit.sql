-- Tags: no-fasttest
-- no-fasttest: needs enough rows of moderate SQL text that an unpatched parse loop overshoots the limit.

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

SET max_execution_time = 1;
SET allow_fuzz_query_functions = 1;

-- 1. formatQuery: many rows of moderate SQL text in one block.
SELECT sum(length(formatQuery('SELECT ' || toString(number) || ' WHERE x=0' || repeat(' OR (y = 1)', 40)))) /* 04691_timed */
FROM numbers(200000) FORMAT Null SETTINGS max_block_size = 200000, max_threads = 1; -- { serverError TIMEOUT_EXCEEDED }

-- 2. formatQueryOrNull must raise, not swallow the cancellation into NULLs. Its per-row `catch (...)`
--    turns any exception into NULL and continues, so the check has to sit outside that handler.
SELECT sum(length(formatQueryOrNull('SELECT ' || toString(number) || ' WHERE x=0' || repeat(' OR (y = 1)', 40)))) /* 04691_timed */
FROM numbers(200000) FORMAT Null SETTINGS max_block_size = 200000, max_threads = 1; -- { serverError TIMEOUT_EXCEEDED }

-- 3. The single-line variants share the same loop.
SELECT sum(length(formatQuerySingleLine('SELECT ' || toString(number) || ' WHERE x=0' || repeat(' OR (y = 1)', 40)))) /* 04691_timed */
FROM numbers(200000) FORMAT Null SETTINGS max_block_size = 200000, max_threads = 1; -- { serverError TIMEOUT_EXCEEDED }

SELECT sum(length(formatQuerySingleLineOrNull('SELECT ' || toString(number) || ' WHERE x=0' || repeat(' OR (y = 1)', 40)))) /* 04691_timed */
FROM numbers(200000) FORMAT Null SETTINGS max_block_size = 200000, max_threads = 1; -- { serverError TIMEOUT_EXCEEDED }

-- 4. Siblings with the same per-row parse loop.
SELECT sum(length(parseQueryToJSON('SELECT ' || toString(number) || ' WHERE x=0' || repeat(' OR (y = 1)', 40)))) /* 04691_timed */
FROM numbers(200000) FORMAT Null SETTINGS max_block_size = 200000, max_threads = 1; -- { serverError TIMEOUT_EXCEEDED }

--    `highlightQuery` and `tokenizeQuery` share one row loop in FunctionQueryTokenization.
--    `highlightQuery` runs a full parse per row and covers that loop. `tokenizeQuery` is lexer-only and
--    its cost is dominated by the size of the token array it builds, so it exhausts memory well before
--    it overshoots a time limit; it gets no case of its own rather than one whose oracle cannot redden.
SELECT sum(length(highlightQuery('SELECT ' || toString(number) || ' WHERE x=0' || repeat(' OR (y = 1)', 40)))) /* 04691_timed */
FROM numbers(60000) FORMAT Null SETTINGS max_block_size = 200000, max_threads = 1; -- { serverError TIMEOUT_EXCEEDED }

-- 5. `fuzzQuery` has one row loop per argument shape and both need the poll.
--    Non-constant argument, so this one takes the ColumnString loop.
SELECT sum(length(fuzzQuery('SELECT ' || toString(number) || ' WHERE x=0' || repeat(' OR (y = 1)', 40)))) /* 04691_timed */
FROM numbers(200000) FORMAT Null SETTINGS max_block_size = 200000, max_threads = 1; -- { serverError TIMEOUT_EXCEEDED }

--    Constant argument: `fuzzQuery` opts out of the default constant handling, so this reaches the
--    separate ColumnConst loop. It is not folded away because the function is non-deterministic.
SELECT sum(length(fuzzQuery('SELECT 1 WHERE x=0' || repeat(' OR (y = 1)', 40)))) /* 04691_timed */
FROM numbers(200000) FORMAT Null SETTINGS max_block_size = 200000, max_threads = 1; -- { serverError TIMEOUT_EXCEEDED }

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
--    directions, so the latency bound in case 7 is what discriminates: 69783 ms pre-fix, 1045 ms with
--    it. The marker is separate because a successful query lands in `query_log` as `QueryFinish`.
SELECT sum(length(formatQuery('SELECT ' || toString(number) || ' WHERE x=0' || repeat(' OR (y = 1)', 40)))) /* 04691_break */
FROM numbers(200000) FORMAT Null
SETTINGS max_block_size = 200000, max_threads = 1, timeout_overflow_mode = 'break';

-- 7. The real assertion: every case above stopped near its limit rather than running its block out.
--    Pre-fix they report 21000-90000 ms against a 1000 ms limit; with the fix they report ~1000 ms.
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

SELECT 'all bounded', count() = 10 AND countIf(query_duration_ms > 15000) = 0
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
--    is charged in raw bytes, so a padded row buys the same crossings with far fewer rows than a
--    bare 'SELECT 1' would: 20000 rows of 39 bytes cross the 64 KiB stride 12 times over.
--    `fuzzQuery` is non-deterministic, so only the length is asserted.
SELECT sum(length(formatQuery(materialize('SELECT 1 -- ' || repeat('x', 27))))) > 0 FROM numbers(20000)
SETTINGS max_execution_time = 0, max_block_size = 200000;
SELECT sum(length(parseQueryToJSON(materialize('SELECT 1 -- ' || repeat('x', 27))))) > 0 FROM numbers(20000)
SETTINGS max_execution_time = 0, max_block_size = 200000;
SELECT sum(length(highlightQuery(materialize('SELECT 1 -- ' || repeat('x', 27))))) > 0 FROM numbers(20000)
SETTINGS max_execution_time = 0, max_block_size = 200000;
SELECT sum(length(tokenizeQuery(materialize('SELECT 1 -- ' || repeat('x', 27))))) > 0 FROM numbers(20000)
SETTINGS max_execution_time = 0, max_block_size = 200000;
SELECT sum(length(fuzzQuery(materialize('SELECT 1 -- ' || repeat('x', 27))))) > 0 FROM numbers(20000)
SETTINGS max_execution_time = 0, max_block_size = 200000;
WITH parseQueryToJSON('SELECT 1 -- ' || repeat('x', 27)) AS json
SELECT sum(length(formatQueryFromJSON(materialize(json)))) > 0 FROM numbers(20000)
SETTINGS max_execution_time = 0, max_block_size = 200000;

--    The two-argument form takes a separate branch, so without a line of its own nothing would redden
--    if the check threw unconditionally there.
WITH parseQueryToJSON('SELECT 1 -- ' || repeat('x', 27)) AS json
SELECT sum(length(formatQueryFromJSON(materialize(json), materialize('SELECT 1 -- ' || repeat('x', 27))))) > 0
FROM numbers(20000) SETTINGS max_execution_time = 0, max_block_size = 200000;

-- 10. The poll must read the EXECUTING query, not the one that built the function. `ALTER` rebuilds the
--     partition key from its own query context and `adjustPartitionKey` hands that same object to later
--     inserts, rebuilding only when the key contains `modulo`, hence the plain key here. Reading a
--     per-instance `QueryStatus` would fail this insert on the ALTER's expired limit, so the limit only
--     has to be finite and elapse, not tight: a tight one can time the ALTER itself out on a loaded host.
--     The insert only needs one stride crossing for the poll to fire, and padded rows reach it with few
--     enough rows to keep the partition count low.
DROP TABLE IF EXISTS t_04691_retained;
CREATE TABLE t_04691_retained (q String, n UInt64)
ENGINE = MergeTree PARTITION BY cityHash64(formatQuery(q)) ORDER BY n;
ALTER TABLE t_04691_retained ADD COLUMN extra UInt8 DEFAULT 0 SETTINGS max_execution_time = 3;
SELECT sleep(3), sleep(1) FORMAT Null SETTINGS max_execution_time = 0;
INSERT INTO t_04691_retained (q, n)
SELECT 'SELECT ' || toString(number % 4) || ' -- ' || repeat('x', 700), number FROM numbers(200)
SETTINGS max_execution_time = 0, max_block_size = 200000;
SELECT count() FROM t_04691_retained SETTINGS max_execution_time = 0;
DROP TABLE t_04691_retained;

SELECT 'ok';
