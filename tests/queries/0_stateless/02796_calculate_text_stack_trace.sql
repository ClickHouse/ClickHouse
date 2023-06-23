-- Tags: no-parallel

TRUNCATE TABLE system.text_log;

SELECT 'Hello', throwIf(1); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
SYSTEM FLUSH LOGS;
SELECT length(stack_trace) > 1000 FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT \'Hello\', throwIf(1)%' AND query NOT LIKE '%system%' ORDER BY event_time_microseconds DESC LIMIT 1;
SELECT message LIKE '%Stack trace%' FROM system.text_log WHERE level = 'Error' AND message LIKE '%Exception%throwIf%' ORDER BY event_time_microseconds DESC LIMIT 10;

TRUNCATE TABLE system.text_log;

SET calculate_text_stack_trace = 0;
SELECT 'World', throwIf(1); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
SYSTEM FLUSH LOGS;
SELECT length(stack_trace) FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT \'World\', throwIf(1)%' AND query NOT LIKE '%system%' ORDER BY event_time_microseconds DESC LIMIT 1;
SELECT message LIKE '%Stack trace%' FROM system.text_log WHERE level = 'Error' AND message LIKE '%Exception%throwIf%' ORDER BY event_time_microseconds DESC LIMIT 10;
