SET log_queries = 1;

SELECT test_function(number, 0) FROM numbers(100) FORMAT Null SETTINGS max_threads = 1, max_block_size = 1;
SELECT test_function(number, 0) FROM numbers(200) FORMAT Null SETTINGS max_threads = 1, max_block_size = 1;

SYSTEM FLUSH LOGS query_log;

SELECT
    count() AS queries,
    min(memory_usage) < 20000000 AS min_less_then_20mb,
    max(memory_usage) < 20000000 AS max_less_then_20mb
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND query_kind = 'Select'
  AND query LIKE '%test_function(number, 0)%'
FORMAT Vertical;

