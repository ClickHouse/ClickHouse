-- Tags: no-parallel, no-llvm-coverage
-- `SYSTEM FLUSH LOGS` flushes every system log table and must wait (up to the 180s
-- `waitFlush` timeout in `SystemLogBase.cpp`) for the shared backlog accumulated in the
-- server-wide log queues (`query_metric_log`, `trace_log`, ...). Under many concurrent
-- parallel tests that backlog can exceed 180s to drain, producing:
--   Code: 159. DB::Exception: Timeout exceeded (180 s) while flushing system log
--   'DB::SystemLogQueue<DB::QueryMetricLogElement>'. (TIMEOUT_EXCEEDED)
-- no-parallel: run in the sequential phase (after the parallel flood ends) so no other
-- test is feeding the shared queues while this test flushes.
-- no-llvm-coverage: coverage instrumentation slows the flush enough to time out even on
-- the residual backlog; skip entirely there.
-- Same pattern as `01473_event_time_microseconds` / `00974_query_profiler` /
-- `01569_query_profiler_big_query_id`.

SYSTEM FLUSH LOGS /* all tables */;

-- Check for system tables which have non-default sorting key
WITH
    ['asynchronous_metric_log', 'asynchronous_insert_log', 'opentelemetry_span_log', 'coverage_log'] AS known_tables,
    'event_date, event_time' as default_sorting_key
SELECT
    'Table ' || name || ' has non-default sorting key: ' || sorting_key
FROM system.tables
WHERE (database = 'system') AND (engine = 'MergeTree') AND name not like 'minio%' AND (NOT arraySum(arrayMap(x -> position(name, x), known_tables))) AND (sorting_key != default_sorting_key);
