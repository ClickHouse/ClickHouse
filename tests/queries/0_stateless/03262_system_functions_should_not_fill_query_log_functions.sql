-- Project only the non-Nullable `name` column. SELECT * would project the
-- Nullable(...) columns,
-- and the analyzer injects null-handling helpers (assumeNotNull, isNotNull, multiIf, ...)
-- for those — they would then leak into used_functions and defeat the test's purpose.
SELECT name FROM system.functions WHERE name = 'bitShiftLeft' format Null;
SYSTEM FLUSH LOGS query_log;
SELECT used_aggregate_functions, used_functions, used_table_functions
FROM system.query_log
WHERE
    event_date >= yesterday() AND event_time >= now() - 600
    AND type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND query LIKE '%bitShiftLeft%';
