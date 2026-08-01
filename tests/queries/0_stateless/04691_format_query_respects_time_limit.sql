-- Tags: no-fasttest
-- no-fasttest: needs enough rows of moderate SQL text that an unpatched parse loop overshoots the limit.

-- `formatQuery` and friends parse one row at a time inside a single pipeline task. Cancellation is only
-- polled between tasks, so before the fix a whole block ran to completion and the query outlived
-- `max_execution_time` and `KILL QUERY` (measured: 23 s under a 1 s limit; 1247 s in CI under MSan).
--
-- Note the assertion is on the ELAPSED TIME, not on the error: an unpatched server also raises
-- TIMEOUT_EXCEEDED, just tens of seconds late, so `-- { serverError TIMEOUT_EXCEEDED }` alone would
-- pass without the fix. Each case below therefore reports whether it stopped anywhere near the limit.

SET max_execution_time = 1;
SET allow_fuzz_query_functions = 1;

-- 1. formatQuery: many rows of moderate SQL text in one block.
SELECT sum(length(formatQuery('SELECT ' || toString(number) || ' WHERE x=0' || repeat(' OR (y = 1)', 40))))
FROM numbers(200000) FORMAT Null; -- { serverError TIMEOUT_EXCEEDED }

-- 2. formatQueryOrNull must raise, not swallow the cancellation into NULLs. Its per-row `catch (...)`
--    turns any exception into NULL and continues, so the check has to sit outside that handler.
SELECT sum(length(formatQueryOrNull('SELECT ' || toString(number) || ' WHERE x=0' || repeat(' OR (y = 1)', 40))))
FROM numbers(200000) FORMAT Null; -- { serverError TIMEOUT_EXCEEDED }

-- 3. The single-line variants share the same loop.
SELECT sum(length(formatQuerySingleLine('SELECT ' || toString(number) || ' WHERE x=0' || repeat(' OR (y = 1)', 40))))
FROM numbers(200000) FORMAT Null; -- { serverError TIMEOUT_EXCEEDED }

SELECT sum(length(formatQuerySingleLineOrNull('SELECT ' || toString(number) || ' WHERE x=0' || repeat(' OR (y = 1)', 40))))
FROM numbers(200000) FORMAT Null; -- { serverError TIMEOUT_EXCEEDED }

-- 4. Siblings with the same per-row parse loop.
SELECT sum(length(parseQueryToJSON('SELECT ' || toString(number) || ' WHERE x=0' || repeat(' OR (y = 1)', 40))))
FROM numbers(200000) FORMAT Null; -- { serverError TIMEOUT_EXCEEDED }

SELECT sum(length(fuzzQuery('SELECT ' || toString(number) || ' WHERE x=0' || repeat(' OR (y = 1)', 40))))
FROM numbers(200000) FORMAT Null; -- { serverError TIMEOUT_EXCEEDED }

SELECT sum(length(formatQueryFromJSON(parseQueryToJSON('SELECT ' || toString(number) || ' WHERE x=0' || repeat(' OR (y = 1)', 40)))))
FROM numbers(200000) FORMAT Null; -- { serverError TIMEOUT_EXCEEDED }

-- 5. The real assertion: every case above stopped near its limit rather than running the block out.
--    Pre-fix these report ~23000 ms against a 1000 ms limit; with the fix they report ~1000 ms.
SYSTEM FLUSH LOGS query_log;

-- enable_parallel_replicas = 0: the test runner can inject parallel replicas, which this local
-- system-table read must not go through.
SELECT 'all bounded', countIf(query_duration_ms > 15000) = 0
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type != 'QueryStart'
  AND event_time > now() - INTERVAL 10 MINUTE
  AND query LIKE '%OR (y = 1)%'
  AND query LIKE '%FROM numbers(200000)%'
SETTINGS enable_parallel_replicas = 0;

-- 6. Results are unchanged when no limit is hit, including the OrNull NULL-on-parse-error contract.
SELECT formatQuery('select    a,b from  t') SETTINGS max_execution_time = 0;
SELECT formatQuerySingleLine('select    a,b from  t') SETTINGS max_execution_time = 0;
SELECT formatQueryOrNull('this is not a query') IS NULL SETTINGS max_execution_time = 0;
SELECT length(parseQueryToJSON('SELECT 1')) > 0 SETTINGS max_execution_time = 0;

SELECT 'ok';
