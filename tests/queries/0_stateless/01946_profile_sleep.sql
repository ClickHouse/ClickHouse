SET log_queries=1;
SET log_profile_events=true;

SELECT 'SLEEP #1 TEST', sleep(0.001) FORMAT Null;
SYSTEM FLUSH LOGS;
SELECT 'SLEEP #1 CHECK', ProfileEvents['SleepFunctionCalls'] as calls, ProfileEvents['SleepFunctionMicroseconds'] as microseconds
FROM system.query_log
WHERE query like '%SELECT ''SLEEP #1 TEST''%'
  AND type > 1
  AND current_database = currentDatabase()
  AND event_date >= yesterday()
    FORMAT JSONEachRow;

SELECT 'SLEEP #2 TEST', sleep(0.001) FROM numbers(2) FORMAT Null;
SYSTEM FLUSH LOGS;
SELECT 'SLEEP #2 CHECK', ProfileEvents['SleepFunctionCalls'] as calls, ProfileEvents['SleepFunctionMicroseconds'] as microseconds
FROM system.query_log
WHERE query like '%SELECT ''SLEEP #2 TEST''%'
  AND type > 1
  AND current_database = currentDatabase()
  AND event_date >= yesterday()
    FORMAT JSONEachRow;

SELECT 'SLEEP #3 TEST', sleepEachRow(0.001) FORMAT Null;
SYSTEM FLUSH LOGS;
SELECT 'SLEEP #3 CHECK', ProfileEvents['SleepFunctionCalls'] as calls, ProfileEvents['SleepFunctionMicroseconds'] as microseconds
FROM system.query_log
WHERE query like '%SELECT ''SLEEP #3 TEST''%'
  AND type > 1
  AND current_database = currentDatabase()
  AND event_date >= yesterday()
    FORMAT JSONEachRow;

SELECT 'SLEEP #4 TEST', sleepEachRow(0.001) FROM numbers(2) FORMAT Null;
SYSTEM FLUSH LOGS;
SELECT 'SLEEP #4 CHECK', ProfileEvents['SleepFunctionCalls'] as calls, ProfileEvents['SleepFunctionMicroseconds'] as microseconds
FROM system.query_log
WHERE query like '%SELECT ''SLEEP #4 TEST''%'
  AND type > 1
  AND current_database = currentDatabase()
  AND event_date >= yesterday()
    FORMAT JSONEachRow;


CREATE VIEW sleep_view AS SELECT sleepEachRow(0.001) FROM system.numbers;
SYSTEM FLUSH LOGS;
SELECT 'SLEEP #5 CHECK', ProfileEvents['SleepFunctionCalls'] as calls, ProfileEvents['SleepFunctionMicroseconds'] as microseconds
FROM system.query_log
WHERE query like '%CREATE VIEW sleep_view AS%'
  AND type > 1
  AND current_database = currentDatabase()
  AND event_date >= yesterday()
    FORMAT JSONEachRow;

SELECT 'SLEEP #6 TEST', sleepEachRow(0.001) FROM sleep_view LIMIT 10 FORMAT Null;
SYSTEM FLUSH LOGS;
SELECT 'SLEEP #6 CHECK', ProfileEvents['SleepFunctionCalls'] as calls, ProfileEvents['SleepFunctionMicroseconds'] as microseconds
FROM system.query_log
WHERE query like '%SELECT ''SLEEP #6 TEST''%'
  AND type > 1
  AND current_database = currentDatabase()
  AND event_date >= yesterday()
    FORMAT JSONEachRow;

DROP TABLE sleep_view;
