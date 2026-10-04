-- `processors_profile_log` must have entries for a query that fails during execution.

SELECT throwIf(number = 3, 'processors_profile_log_on_exception') FROM numbers(10) FORMAT Null SETTINGS log_processors_profiles = 1; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

SYSTEM FLUSH LOGS query_log, processors_profile_log;

WITH
    (
        SELECT query_id
        FROM system.query_log
        WHERE event_date >= yesterday()
            AND current_database = currentDatabase()
            AND type = 'ExceptionWhileProcessing'
            AND log_comment = '05235_processors_profile_log_on_exception.sql-' || currentDatabase()
        ORDER BY event_time_microseconds DESC
        LIMIT 1
    ) AS failed_query_id
SELECT
    count() > 0,
    countIf(output_rows > 0) > 0,
    countIf(exception_code = 395 AND exception LIKE '%processors_profile_log_on_exception%') = count()
FROM system.processors_profile_log
WHERE event_date >= yesterday() AND query_id = failed_query_id;
