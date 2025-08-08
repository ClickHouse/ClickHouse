SELECT * FROM system.functions WHERE name = 'bitShiftLeft' format Null;
SYSTEM FLUSH LOGS query_log;
SELECT used_aggregate_functions, used_functions, used_table_functions
FROM system.query_log
WHERE
    event_date >= yesterday()
    AND type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND query LIKE '%bitShiftLeft%';
