SELECT '#02136_scalar_subquery_1', (SELECT max(number) FROM numbers(1000)) as n;
SELECT '#02136_scalar_subquery_2', (SELECT max(number) FROM numbers(1000)) as n, (SELECT min(number) FROM numbers(1000)) as n2;
SELECT '#02136_scalar_subquery_3', (SELECT max(number) FROM numbers(1000)) as n, (SELECT max(number) FROM numbers(1000)) as n2; -- Cached
SELECT '#02136_scalar_subquery_4', (SELECT max(number) FROM numbers(1000)) as n FROM system.numbers LIMIT 2; -- Cached

SYSTEM FLUSH LOGS query_log;
SELECT read_rows, query FROM system.query_log
WHERE
      event_date >= yesterday()
  AND type = 'QueryFinish'
  AND current_database == currentDatabase()
  AND query LIKE 'SELECT ''#02136_scalar_subquery_%'
ORDER BY query ASC;
