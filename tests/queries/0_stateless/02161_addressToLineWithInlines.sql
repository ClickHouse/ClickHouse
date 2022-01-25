-- Tags: no-tsan, no-asan, no-ubsan, no-msan, no-debug


SELECT addressToLineWithInlines(1); -- { serverError 446 }

SET allow_introspection_functions = 1;
SET query_profiler_real_time_period_ns = 0;
SET query_profiler_cpu_time_period_ns = 1000000;
SET log_queries = 1;
SELECT count() FROM numbers_mt(10000000000) SETTINGS log_comment='02161_test_case';
SET log_queries = 0;
SET query_profiler_cpu_time_period_ns = 0;
SYSTEM FLUSH LOGS;

WITH
    address_list AS
    (
        SELECT DISTINCT arrayJoin(trace) AS address FROM system.trace_log WHERE query_id =
        (
            SELECT query_id FROM system.query_log WHERE current_database = currentDatabase() AND log_comment='02161_test_case' ORDER BY event_time DESC LIMIT 1
        )
    )
SELECT 'has inlines:', max(length(addressToLineWithInlines(address))) > 1 FROM address_list;

