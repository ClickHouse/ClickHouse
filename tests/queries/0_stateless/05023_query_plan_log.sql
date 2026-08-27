-- Tags: no-old-analyzer

-- Verifies system.query_plan_log: which queries are captured, which are not, and that the
-- captured row contains the plan, its runtime statistics and correct metadata.
--
-- Only InterpreterSelectQueryAnalyzer captures plans, so nothing at all is logged with
-- `enable_analyzer = 0` and every assertion below would fail.
--
-- Rows are matched by joining on query_id against system.query_log restricted to
-- currentDatabase(), so the assertions only see queries issued by this run of this test.
-- Matching on query_string alone would also pick up rows left by an earlier run against the
-- same server, or by another test running in parallel, since system.query_plan_log is
-- server-wide and has no current_database column of its own.
--
-- Markers are matched with position() rather than LIKE so that `_` stays a literal character.

DROP TABLE IF EXISTS qpl_insert_target;
CREATE TABLE qpl_insert_target (x UInt64) ENGINE = Memory;

-- Nothing is captured while the setting is off.
SET log_query_plans = 0;
SELECT count() FROM numbers(1000) WHERE number > 900 AND '05023_off' != '' FORMAT Null;

SET log_query_plans = 1;

-- A plain SELECT is captured. A million rows keeps the per-step timings well above the
-- resolution of the clock, so the statistics assertions below are not racing the noise floor.
SELECT number FROM numbers(1000000) WHERE number % 7 = 0 AND '05023_finish' != '' ORDER BY number DESC LIMIT 3 FORMAT Null;

-- Nested interpreters must not produce one row each.
SELECT max(x) FROM (SELECT number AS x FROM numbers(100) WHERE number > 10 AND '05023_subquery' != '') FORMAT Null;

-- A join reports its own metrics (matched rows, hash table size) only when the query runs with the
-- analyze mode on, which only EXPLAIN ANALYZE turns on. The plan of an ordinary query must
-- therefore render without them: `parallel_hash` does not even allocate the object holding those
-- counters, so asking the join for them used to abort the server.
SELECT count() FROM (SELECT number AS k FROM numbers(100000)) AS l
JOIN (SELECT number AS k FROM numbers(1000)) AS r USING (k)
WHERE '05023_join' != ''
SETTINGS join_algorithm = 'parallel_hash' FORMAT Null;

-- EXPLAIN ANALYZE runs through InterpreterExplainQuery, which does not support plan profiling.
-- It is wrapped in a subquery so its output (which contains timings) is discarded and the test
-- stays deterministic. The wrapper is an ordinary SELECT and is captured itself, so the assertion
-- below checks that no captured row is an EXPLAIN statement, rather than counting rows.
SELECT ignore(*) FROM (EXPLAIN ANALYZE SELECT count() FROM numbers(1000) WHERE '05023_explain' != '') FORMAT Null;

-- INSERT ... SELECT is out of scope: the top-level interpreter is InterpreterInsertQuery.
INSERT INTO qpl_insert_target SELECT number FROM numbers(10) WHERE '05023_insert' != '';

-- A query failing during execution is captured, with the plan it was running.
SELECT throwIf(number = 5, '05023_throw') FROM numbers(10); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

-- A query failing before any plan exists leaves no row.
SELECT * FROM qpl_missing_05023_table; -- { serverError UNKNOWN_TABLE }

-- A recursive CTE evaluates its inner query through a second InterpreterSelectQueryAnalyzer on the
-- same query context, from inside RecursiveCTESource — that is, after the outer plan was captured.
-- The captured row must still describe the outer query and keep its statistics.
WITH RECURSIVE qpl_cte AS
(
  SELECT 1 AS n
  UNION ALL
  SELECT n + 1 FROM qpl_cte WHERE n < 5
)
SELECT sum(n) FROM qpl_cte WHERE '05023_recursive' != '' FORMAT Null;

SET log_query_plans = 0;
SYSTEM FLUSH LOGS query_log;
SYSTEM FLUSH LOGS query_plan_log;

SELECT 'off', count()
FROM system.query_plan_log
WHERE query_id IN (SELECT query_id FROM system.query_log WHERE current_database = currentDatabase())
  AND position(query_string, '05023_off') > 0;

SELECT 'explain_analyze', countIf(position(query_string, 'EXPLAIN') = 1)
FROM system.query_plan_log
WHERE query_id IN (SELECT query_id FROM system.query_log WHERE current_database = currentDatabase())
  AND position(query_string, '05023_explain') > 0;

SELECT 'insert_select', count()
FROM system.query_plan_log
WHERE query_id IN (SELECT query_id FROM system.query_log WHERE current_database = currentDatabase())
  AND position(query_string, '05023_insert') > 0;

SELECT 'before_start', count()
FROM system.query_plan_log
WHERE query_id IN (SELECT query_id FROM system.query_log WHERE current_database = currentDatabase())
  AND position(query_string, 'qpl_missing_05023_table') > 0;

SELECT 'subquery', count()
FROM system.query_plan_log
WHERE query_id IN (SELECT query_id FROM system.query_log WHERE current_database = currentDatabase())
  AND position(query_string, '05023_subquery') > 0;

-- `parallelism Unknown` is printed for every step when the per-step wall clocks were never
-- started, so its absence is what proves the StepWallClockRegistry reached the executor.
-- Checking for the presence of the word `parallelism` would pass either way.
SELECT
    'finish',
    count(),
    anyLast(status),
    anyLast(normalized_query_hash) != 0,
    anyLast(revision) != 0,
    anyLast(query_start_time_microseconds) > toDateTime64('1971-01-01 00:00:00', 6),
    anyLast(event_time_microseconds) >= anyLast(query_start_time_microseconds),
    anyLast(position(ascii_plan, 'ReadFromSystemNumbers')) > 0,
    anyLast(position(ascii_plan, 'Filter column')) > 0,
    anyLast(position(ascii_plan, 'I/O: rows')) > 0,
    anyLast(position(ascii_plan, 'parallelism Unknown')) = 0
FROM system.query_plan_log
WHERE query_id IN (SELECT query_id FROM system.query_log WHERE current_database = currentDatabase())
  AND position(query_string, '05023_finish') > 0;

-- The join step is rendered with its stages and I/O, just without the join-specific metrics.
SELECT
    'join',
    count(),
    anyLast(position(ascii_plan, 'Join')) > 0,
    anyLast(position(ascii_plan, 'I/O: rows')) > 0,
    anyLast(position(ascii_plan, 'unique keys')) = 0
FROM system.query_plan_log
WHERE query_id IN (SELECT query_id FROM system.query_log WHERE current_database = currentDatabase())
  AND position(query_string, '05023_join') > 0;

-- A failed query keeps its plan but has no statistics: BlockIO::onException runs the exception
-- callbacks and resets the pipeline without ever calling the finalize callback, which is where
-- the statistics are collected.
SELECT
    'exception',
    count(),
    anyLast(status),
    anyLast(length(ascii_plan)) > 0,
    anyLast(position(ascii_plan, 'I/O: rows')) = 0
FROM system.query_plan_log
WHERE query_id IN (SELECT query_id FROM system.query_log WHERE current_database = currentDatabase())
  AND position(query_string, '05023_throw') > 0;

SELECT
    'recursive_cte',
    count(),
    anyLast(position(ascii_plan, 'I/O: rows')) > 0,
    anyLast(position(ascii_plan, 'parallelism Unknown')) = 0
FROM system.query_plan_log
WHERE query_id IN (SELECT query_id FROM system.query_log WHERE current_database = currentDatabase())
  AND position(query_string, '05023_recursive') > 0;

DROP TABLE qpl_insert_target;
