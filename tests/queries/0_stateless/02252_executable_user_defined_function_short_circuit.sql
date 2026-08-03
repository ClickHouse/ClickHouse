SELECT number FROM numbers(10) WHERE number > 15 and test_function(number, number) == 4;

SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['ExecuteShellCommand'] FROM system.query_log WHERE
    current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query == 'SELECT number FROM numbers(10) WHERE number > 15 and test_function(number, number) == 4;'
    AND event_date >= yesterday() AND event_time > now() - interval 10 minute
    LIMIT 1;
