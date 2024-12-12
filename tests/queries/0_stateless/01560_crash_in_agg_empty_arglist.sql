-- make sure the system.query_log table is created
SELECT 1;
SYSTEM FLUSH LOGS;

SELECT any() as t, substring(query, 1, 70) AS query, avg(memory_usage) usage, count() count FROM system.query_log WHERE current_database = currentDatabase() AND event_date >= toDate(1604295323) AND event_time >= toDateTime(1604295323) AND type in (1,2,3,4) and initial_user in ('') and('all' = 'all' or(positionCaseInsensitive(query, 'all') = 1)) GROUP BY query ORDER BY usage desc LIMIT 5; -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
