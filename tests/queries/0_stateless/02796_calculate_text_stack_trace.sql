SELECT throwIf(1); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
SYSTEM FLUSH LOGS;
SELECT length(stack_trace) > 1000 FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT throwIf(1)%' AND query NOT LIKE '%system%' ORDER BY event_time_microseconds DESC LIMIT 1;

SET calculate_text_stack_trace = 0;
SELECT throwIf(1); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
SYSTEM FLUSH LOGS;
SELECT length(stack_trace) FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT throwIf(1)%' AND query NOT LIKE '%system%' ORDER BY event_time_microseconds DESC LIMIT 1;
