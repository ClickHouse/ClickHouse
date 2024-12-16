SET log_queries=0;
SET log_query_threads=0;

-- SETUP TABLES
CREATE TABLE table_a (a String, b Int64) ENGINE = MergeTree ORDER BY b;
CREATE TABLE table_b (a Float64,  b Int64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE table_c (a Float64) ENGINE = MergeTree ORDER BY a;

CREATE TABLE table_d (a Float64, count Int64) ENGINE MergeTree ORDER BY a;
CREATE TABLE table_e (a Float64, count Int64) ENGINE MergeTree ORDER BY a;
CREATE TABLE table_f (a Float64, count Int64) ENGINE MergeTree ORDER BY a;

-- SETUP MATERIALIZED VIEWS
CREATE MATERIALIZED VIEW matview_a_to_b TO table_b AS SELECT toFloat64(a) AS a, b + sleepEachRow(0.000001) AS count FROM table_a;
CREATE MATERIALIZED VIEW matview_b_to_c TO table_c AS SELECT SUM(a + sleepEachRow(0.000002)) as a FROM table_b;
CREATE MATERIALIZED VIEW matview_join_d_e TO table_f AS SELECT table_d.a as a, table_e.count + sleepEachRow(0.000003) as count FROM table_d LEFT JOIN table_e ON table_d.a = table_e.a;

-- ENABLE LOGS
SET parallel_view_processing=0;
SET log_query_views=1;
SET log_queries_min_type='QUERY_FINISH';
SET log_queries=1;

-- INSERT 1
INSERT INTO table_a SELECT '111', * FROM numbers(100);

-- INSERT 2
INSERT INTO table_d SELECT 0.5, * FROM numbers(50);

SYSTEM FLUSH LOGS;


-- CHECK LOGS OF INSERT 1
SELECT
    'Query log rows' as stage,
    read_rows,
    written_rows,
    arraySort(databases) as databases,
    arraySort(tables) as tables,
    arraySort(views) as views,
    ProfileEvents['SleepFunctionCalls'] as sleep_calls,
    ProfileEvents['SleepFunctionMicroseconds'] as sleep_us,
    ProfileEvents['SelectedRows'] as profile_select_rows,
    ProfileEvents['SelectedBytes'] as profile_select_bytes,
    ProfileEvents['InsertedRows'] as profile_insert_rows,
    ProfileEvents['InsertedBytes'] as profile_insert_bytes
FROM system.query_log
WHERE query like '-- INSERT 1%INSERT INTO table_a%'
  AND current_database = currentDatabase()
  AND event_date >= yesterday()
FORMAT Vertical;

SELECT
    'Depending views' as stage,
    view_name,
    view_type,
    status,
    view_target,
    view_query,
    read_rows,
    written_rows,
    ProfileEvents['SleepFunctionCalls'] as sleep_calls,
    ProfileEvents['SleepFunctionMicroseconds'] as sleep_us,
    ProfileEvents['SelectedRows'] as profile_select_rows,
    ProfileEvents['SelectedBytes'] as profile_select_bytes,
    ProfileEvents['InsertedRows'] as profile_insert_rows,
    ProfileEvents['InsertedBytes'] as profile_insert_bytes
FROM system.query_views_log
WHERE initial_query_id =
      (
          SELECT initial_query_id
          FROM system.query_log
          WHERE query like '-- INSERT 1%INSERT INTO table_a%'
            AND current_database = currentDatabase()
            AND event_date >= yesterday()
          LIMIT 1
      )
ORDER BY view_name
FORMAT Vertical;

-- CHECK LOGS OF INSERT 2
SELECT
    'Query log rows 2' as stage,
    read_rows,
    written_rows,
    arraySort(databases) as databases,
    arraySort(tables) as tables,
    arraySort(views) as views,
    ProfileEvents['SleepFunctionCalls'] as sleep_calls,
    ProfileEvents['SleepFunctionMicroseconds'] as sleep_us,
    ProfileEvents['SelectedRows'] as profile_select_rows,
    ProfileEvents['SelectedBytes'] as profile_select_bytes,
    ProfileEvents['InsertedRows'] as profile_insert_rows,
    ProfileEvents['InsertedBytes'] as profile_insert_bytes
FROM system.query_log
WHERE query like '-- INSERT 2%INSERT INTO table_d%'
  AND current_database = currentDatabase()
  AND event_date >= yesterday()
FORMAT Vertical;

SELECT
    'Depending views 2' as stage,
    view_name,
    view_type,
    status,
    view_target,
    view_query,
    read_rows,
    written_rows,
    ProfileEvents['SleepFunctionCalls'] as sleep_calls,
    ProfileEvents['SleepFunctionMicroseconds'] as sleep_us,
    ProfileEvents['SelectedRows'] as profile_select_rows,
    ProfileEvents['SelectedBytes'] as profile_select_bytes,
    ProfileEvents['InsertedRows'] as profile_insert_rows,
    ProfileEvents['InsertedBytes'] as profile_insert_bytes
FROM system.query_views_log
WHERE initial_query_id =
      (
          SELECT initial_query_id
          FROM system.query_log
          WHERE query like '-- INSERT 2%INSERT INTO table_d%'
            AND current_database = currentDatabase()
            AND event_date >= yesterday()
          LIMIT 1
      )
ORDER BY view_name
FORMAT Vertical;

-- TEARDOWN
DROP TABLE matview_a_to_b;
DROP TABLE matview_b_to_c;
DROP TABLE matview_join_d_e;
DROP TABLE table_f;
DROP TABLE table_e;
DROP TABLE table_d;
DROP TABLE table_c;
DROP TABLE table_b;
DROP TABLE table_a;
