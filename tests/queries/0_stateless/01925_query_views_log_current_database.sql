SET allow_experimental_live_view = 1;
SET log_queries=0;
SET log_query_threads=0;

-- SETUP TABLES
CREATE TABLE table_a (a String, b Int64) ENGINE = MergeTree ORDER BY b;
CREATE TABLE table_b (a Float64,  b Int64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE table_c (a Float64) ENGINE = MergeTree ORDER BY a;

-- SETUP MATERIALIZED VIEWS
CREATE MATERIALIZED VIEW matview_a_to_b TO table_b AS SELECT toFloat64(a) AS a, b AS count FROM table_a;
CREATE MATERIALIZED VIEW matview_b_to_c TO table_c AS SELECT SUM(a) as a FROM table_b;
-- We don't include test materialized views here since the name is based on the uuid on atomic databases
-- and that makes the test harder to read

-- SETUP LIVE VIEW
---- table_b_live_view (Int64)
DROP TABLE IF EXISTS table_b_live_view;
CREATE LIVE VIEW table_b_live_view AS SELECT sum(a + b) FROM table_b;

-- ENABLE LOGS
SET log_query_views=1;
SET log_queries_min_type='QUERY_FINISH';
SET log_queries=1;

-- INSERT 1
INSERT INTO table_a SELECT '111', * FROM numbers(100);
SYSTEM FLUSH LOGS;

SELECT
    'Query log rows' as stage,
    read_rows,
    written_rows,
    arraySort(databases) as databases,
    arraySort(tables) as tables,
    arraySort(views) as views
FROM system.query_log
WHERE
    query like '-- INSERT 1%INSERT INTO table_a%'
    AND current_database = currentDatabase()
    AND event_date >= yesterday()
FORMAT JSONEachRow;

SELECT
    'Depending views' as stage,
    view_name,
    view_target,
    view_query
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
FORMAT JSONEachRow;

-- TEARDOWN
DROP TABLE table_b_live_view;
DROP TABLE matview_a_to_b;
DROP TABLE matview_b_to_c;
DROP TABLE table_b;
DROP TABLE table_a;
