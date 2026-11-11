-- Tags: no-tsan, no-asan, no-ubsan, no-msan, no-debug, no-fasttest, no-llvm-coverage

SET allow_introspection_functions = 0;
SELECT addressToLineWithInlines(1); -- { serverError FUNCTION_NOT_ALLOWED }

SET allow_introspection_functions = 1;
SET query_profiler_real_time_period_ns = 0;
SET query_profiler_cpu_time_period_ns = 1000000;
SET log_queries = 1, max_rows_to_read = 0;
SELECT count() FROM numbers_mt(10000000000) SETTINGS log_comment='02161_test_case';
SET log_queries = 0;
SET query_profiler_cpu_time_period_ns = 0;
SYSTEM FLUSH LOGS query_log, trace_log;

-- Symbolization test, not a perf test: reading system.trace_log can be slow
-- and false-positives the execution-time guards (TOO_SLOW / TIMEOUT_EXCEEDED).
SET max_estimated_execution_time = 0, max_execution_time = 0;

WITH
    lineWithInlines AS
    (
        -- Deduplicate addresses before calling addressToLineWithInlines to avoid
        -- calling the slow DWARF-lookup function for every occurrence of an address
        -- across all trace samples (which can be thousands with certain randomised
        -- settings such as max_threads=1).
        SELECT addressToLineWithInlines(addr) AS lineWithInlines
        FROM
        (
            SELECT DISTINCT arrayJoin(trace) AS addr
            FROM system.trace_log
            WHERE event_date >= yesterday() AND event_time >= now() - 600 AND query_id =
            (
                SELECT query_id FROM system.query_log WHERE event_date >= yesterday() AND event_time >= now() - 600 AND current_database = currentDatabase() AND log_comment='02161_test_case' ORDER BY event_time DESC LIMIT 1
            )
        )
    )
SELECT 'has inlines:', or(max(length(lineWithInlines)) > 1, max(locate(lineWithInlines[1], ':')) = 0) FROM lineWithInlines SETTINGS short_circuit_function_evaluation='enable';
-- `max(length(lineWithInlines)) > 1` check there is any inlines.
-- `max(locate(lineWithInlines[1], ':')) = 0` check whether none could get a symbol.
