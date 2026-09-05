SET log_queries = 1, log_queries_min_type = 'QUERY_START';
SET log_comment = '04490_function_helpers_variadic_argument_error_message';

SELECT replicate(1, [1, 2, 3], 42); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT replicate(1, [1, 2, 3], [4, 5, 6], 42); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SYSTEM FLUSH LOGS query_log;

SELECT countIf(position(exception, '3rd argument \'arrays\' to function \'replicate\'') > 0) = 1
FROM
(
    SELECT exception
    FROM system.query_log
    WHERE current_database = currentDatabase()
        AND event_date >= yesterday()
        AND event_time >= now() - 600
        AND log_comment = '04490_function_helpers_variadic_argument_error_message'
        AND query LIKE 'SELECT replicate(1, [1, 2, 3], 42)%'
        AND exception_code = 43
    ORDER BY event_time_microseconds DESC
    LIMIT 1
);

SELECT countIf(position(exception, '4th argument \'arrays\' to function \'replicate\'') > 0) = 1
FROM
(
    SELECT exception
    FROM system.query_log
    WHERE current_database = currentDatabase()
        AND event_date >= yesterday()
        AND event_time >= now() - 600
        AND log_comment = '04490_function_helpers_variadic_argument_error_message'
        AND query LIKE 'SELECT replicate(1, [1, 2, 3], [4, 5, 6], 42)%'
        AND exception_code = 43
    ORDER BY event_time_microseconds DESC
    LIMIT 1
);
