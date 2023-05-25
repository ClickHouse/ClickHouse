SET log_queries = 1;
SELECT '01461_query_log_query_start_time_milliseconds_test';
SYSTEM FLUSH LOGS;
-- assumes that the query_start_time field is already accurate.
WITH (
      (
          SELECT query_start_time_microseconds
          FROM system.query_log
          WHERE current_database = currentDatabase()
            AND query like 'SELECT \'01461_query%'
            AND event_date >= yesterday()
          ORDER BY query_start_time DESC
          LIMIT 1
      ) AS time_with_microseconds,
      (
          SELECT query_start_time
          FROM system.query_log
          WHERE current_database = currentDatabase()
            AND query like 'SELECT \'01461_query%'
            AND event_date >= yesterday()
          ORDER BY query_start_time DESC
          LIMIT 1
      ) AS t)
SELECT if(dateDiff('second', toDateTime(time_with_microseconds), toDateTime(t)) = 0, 'ok', 'fail'); --

SET log_query_threads = 1;
SELECT '01461_query_thread_log_query_start_time_milliseconds_test';
SYSTEM FLUSH LOGS;
-- assumes that the query_start_time field is already accurate.
WITH (
      (
          SELECT query_start_time_microseconds
          FROM system.query_thread_log
          WHERE current_database = currentDatabase()
            AND query like 'SELECT \'01461_query%'
            AND event_date >= yesterday()
          ORDER BY query_start_time DESC
          LIMIT 1
      ) AS time_with_microseconds,
      (
          SELECT query_start_time
          FROM system.query_thread_log
          WHERE current_database = currentDatabase()
            AND query like 'SELECT \'01461_query%'
            AND event_date >= yesterday()
          ORDER BY query_start_time DESC
          LIMIT 1
      ) AS t)
SELECT if(dateDiff('second', toDateTime(time_with_microseconds), toDateTime(t)) = 0, 'ok', 'fail'); --
