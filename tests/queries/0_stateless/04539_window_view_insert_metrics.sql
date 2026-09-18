-- Window views are not insert dependencies of their source tables. Keep this
-- assertion with metric attribution so a future dependency implementation must
-- explicitly classify its rows.

SET allow_experimental_window_view = 1;
SET allow_experimental_analyzer = 0;
SET log_queries = 1;
SET log_queries_min_type = 'QUERY_FINISH';

DROP TABLE IF EXISTS window_view_metrics_view;
DROP TABLE IF EXISTS window_view_metrics_src;

CREATE TABLE window_view_metrics_src (id UInt64, ts DateTime) ENGINE = MergeTree ORDER BY id;
CREATE WINDOW VIEW window_view_metrics_view ENGINE Memory AS
    SELECT count(id) AS cnt, tumbleStart(window_id) AS window_start
    FROM window_view_metrics_src
    GROUP BY tumble(ts, INTERVAL '5' SECOND) AS window_id;

SELECT countIf(has(dependencies_table, 'window_view_metrics_view'))
FROM system.tables
WHERE database = currentDatabase();

SET log_comment = '04539_window_view';
INSERT INTO window_view_metrics_src SELECT number, toDateTime('2026-01-01 00:00:00') + number FROM numbers(7);
SET log_comment = '';

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['DirectInsertedRows'],
    ProfileEvents['MaterializedViewInsertedRows'],
    ProfileEvents['MaterializedViewInsertedBytes'],
    ProfileEvents['InsertedRows'] = ProfileEvents['DirectInsertedRows'],
    ProfileEvents['InsertedBytes'] = ProfileEvents['DirectInsertedBytes']
FROM system.query_log
WHERE current_database = currentDatabase()
  AND log_comment = '04539_window_view'
  AND query LIKE 'INSERT INTO window_view_metrics_src%'
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
ORDER BY event_time_microseconds DESC
LIMIT 1;

DROP TABLE window_view_metrics_view;
DROP TABLE window_view_metrics_src;
