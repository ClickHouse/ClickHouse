-- Tags: no-fasttest
-- no-fasttest: needs enough rows of moderate SQL text that an unpatched parse loop overshoots the limit.

-- The query-parsing functions parse one row at a time inside a single pipeline task. Cancellation is
-- only polled between tasks, so before the fix a whole block ran to completion and the query outlived
-- `max_execution_time` and `KILL QUERY` (measured: 23 s under a 1 s limit; 1247 s in CI under MSan).
--
-- Note the assertion is on the ELAPSED TIME, not on the error: an unpatched server also raises
-- TIMEOUT_EXCEEDED, just tens of seconds late, so `-- { serverError TIMEOUT_EXCEEDED }` alone would
-- pass without the fix. Case 6 is the load-bearing one and checks how late each case stopped.
--
-- Every timed query pins `max_block_size` and `max_threads`, which the test runner otherwise
-- randomizes: a small block lets an unpatched server finish its uninterruptible unit soon enough to
-- stay under case 6's threshold, silently disarming it. The pins are per query so that they cannot
-- leak into case 6's own read. Each timed query carries the `04691_timed` marker that case 6 counts.

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

-- 6. The real assertion: every case above stopped near its limit rather than running its block out.
--    Pre-fix they report 21000-90000 ms against a 1000 ms limit; with the fix they report ~1000 ms.
--    `count() = 9` keeps the assertion from going vacuous: countIf(...) = 0 over an empty result set is
--    trivially true, so a marker that stopped matching would silently disarm the whole test. The type
--    filter selects exactly the nine timed queries, all of which are expected to have raised.
--    max_execution_time = 0: the session limit above applies to the flush and to this unindexed
--    query_log scan as well, and on a loaded sanitizer host either can exceed one second and time the
--    oracle itself out. SYSTEM FLUSH LOGS takes no SETTINGS clause, hence the session-level SET.
--    enable_parallel_replicas = 0: the test runner can inject parallel replicas, which this local
--    system-table read must not go through.
SET max_execution_time = 0;
SYSTEM FLUSH LOGS query_log;

SELECT 'all bounded', count() = 9 AND countIf(query_duration_ms > 15000) = 0
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'ExceptionWhileProcessing'
  AND event_time > now() - INTERVAL 10 MINUTE
  AND query LIKE '%04691\_timed%'
SETTINGS max_execution_time = 0, enable_parallel_replicas = 0;

-- 7. Results are unchanged when no limit is hit, including the OrNull NULL-on-parse-error contract.
SELECT formatQuery('select    a,b from  t') SETTINGS max_execution_time = 0;
SELECT formatQuerySingleLine('select    a,b from  t') SETTINGS max_execution_time = 0;
SELECT formatQueryOrNull('this is not a query') IS NULL SETTINGS max_execution_time = 0;
SELECT length(parseQueryToJSON('SELECT 1')) > 0 SETTINGS max_execution_time = 0;
SELECT length(highlightQuery('SELECT 1')) > 0 SETTINGS max_execution_time = 0;
SELECT length(tokenizeQuery('SELECT 1')) > 0 SETTINGS max_execution_time = 0;

SELECT 'ok';
