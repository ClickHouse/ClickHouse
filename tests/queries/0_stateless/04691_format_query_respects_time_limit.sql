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

--    The two-argument form also parses the second argument per row, so its bytes have to count
--    toward the stride as well. A small JSON with a large original is the shape that overshoots when
--    only the JSON is counted: the JSON floor (339 bytes for the smallest valid AST) admits 193 rows
--    per stride, each lexing the whole original. `max_query_size` is raised because at its default
--    the original is capped at 262144 bytes, which bounds the overshoot to ~1.7 s -- under case 7's
--    threshold, so the default-settings form cannot discriminate. This case therefore carries its own
--    latency assertion below rather than case 7's marker, since it needs those raised settings. Its
--    marker spells no other case's marker: a comment is part of the logged query text, so mentioning
--    one here would make this query match that case's own count and inflate it.
WITH parseQueryToJSON('SELECT 1') AS json
SELECT sum(length(formatQueryFromJSON(materialize(json), materialize('SELECT 1 WHERE x=0' || repeat(' OR (y = 1)', 110000))))) /* 04691_twoarg */
FROM numbers(200) FORMAT Null
SETTINGS max_block_size = 200000, max_threads = 1, max_query_size = 2000000,
         max_ast_elements = 100000000, max_parser_depth = 1000000; -- { serverError TIMEOUT_EXCEEDED }

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

--    The two-argument case gets its own bound because it needs a raised `max_query_size`, which the
--    cases above deliberately leave at the default. Measured: 4.7-5.3 s before counting the second
--    argument's bytes, 1.08-1.13 s after, against a 1 s limit. 3000 ms sits clear of both.
SELECT 'two-argument bounded', count() = 1 AND countIf(query_duration_ms > 3000) = 0
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type != 'QueryStart'
  AND event_time > now() - INTERVAL 10 MINUTE
  AND query LIKE '%04691\_twoarg%'
SETTINGS max_execution_time = 0, enable_parallel_replicas = 0;

-- 8. Results are unchanged when no limit is hit, including the OrNull NULL-on-parse-error contract.
SELECT formatQuery('select    a,b from  t') SETTINGS max_execution_time = 0;
SELECT formatQuerySingleLine('select    a,b from  t') SETTINGS max_execution_time = 0;
SELECT formatQueryOrNull('this is not a query') IS NULL SETTINGS max_execution_time = 0;

-- 9. The liveness arm: a regression that made the check fire when it should not, or throw
--    unconditionally, has to be caught somewhere, and everywhere else these functions appear
--    TIMEOUT_EXCEEDED is the expected result. One call per polled row loop, no limit set.
--    Each call has to actually REACH the check, and the check is throttled on accumulated input
--    bytes, so a single short query never polls at all and would make this arm vacuous. These run
--    enough rows in one block to cross the stride many times over, so a spurious throw shows up here.
--    `fuzzQuery` is non-deterministic, so only the length is asserted.
SELECT sum(length(formatQuery(materialize('SELECT 1')))) > 0 FROM numbers(100000)
SETTINGS max_execution_time = 0, max_block_size = 200000;
SELECT sum(length(parseQueryToJSON(materialize('SELECT 1')))) > 0 FROM numbers(100000)
SETTINGS max_execution_time = 0, max_block_size = 200000;
SELECT sum(length(highlightQuery(materialize('SELECT 1')))) > 0 FROM numbers(100000)
SETTINGS max_execution_time = 0, max_block_size = 200000;
SELECT sum(length(tokenizeQuery(materialize('SELECT 1')))) > 0 FROM numbers(100000)
SETTINGS max_execution_time = 0, max_block_size = 200000;
SELECT sum(length(fuzzQuery(materialize('SELECT 1')))) > 0 FROM numbers(100000)
SETTINGS max_execution_time = 0, max_block_size = 200000;
WITH parseQueryToJSON('SELECT 1') AS json
SELECT sum(length(formatQueryFromJSON(materialize(json)))) > 0 FROM numbers(100000)
SETTINGS max_execution_time = 0, max_block_size = 200000;

SELECT 'ok';
