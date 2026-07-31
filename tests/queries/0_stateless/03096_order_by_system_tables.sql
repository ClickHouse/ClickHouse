-- Tags: no-llvm-coverage
-- Tag no-llvm-coverage: `SYSTEM FLUSH LOGS` flushes every system log table (including
-- `trace_log`) and must wait for the shared backlog accumulated by other parallel tests.
-- Under LLVM source-based coverage instrumentation the accumulated `trace_log` queue can
-- exceed the 180s server-side `waitFlush` timeout (`SystemLogBase.cpp`), producing:
--   Code: 159. DB::Exception: Timeout exceeded (180 s) while flushing system log
--   'DB::SystemLogQueue<DB::TraceLogElement>'. (TIMEOUT_EXCEEDED)
-- Same pattern as `01473_event_time_microseconds` / `00974_query_profiler` /
-- `01569_query_profiler_big_query_id` and the precedent of commit cec04c17241
-- (Kafka tests under coverage).

SYSTEM FLUSH LOGS /* all tables */;

-- Check for system tables which have non-default sorting key
WITH
    ['asynchronous_metric_log', 'asynchronous_insert_log', 'opentelemetry_span_log', 'coverage_log'] AS known_tables,
    'event_date, event_time' as default_sorting_key
SELECT
    'Table ' || name || ' has non-default sorting key: ' || sorting_key
FROM system.tables
WHERE (database = 'system') AND (engine = 'MergeTree') AND name not like 'minio%' AND (NOT arraySum(arrayMap(x -> position(name, x), known_tables))) AND (sorting_key != default_sorting_key);
