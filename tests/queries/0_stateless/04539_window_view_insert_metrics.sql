SET allow_experimental_window_view = 1;
SET allow_experimental_analyzer = 0;
SET log_queries = 1;
SET log_queries_min_type = 'QUERY_FINISH';

DROP TABLE IF EXISTS window_view_metrics_view;
DROP TABLE IF EXISTS window_view_metrics_src;

CREATE TABLE window_view_metrics_src
(
    num UInt32,
    insertion_time DateTime64(9),
    simulation_time DateTime64(9)
)
ENGINE = MergeTree
ORDER BY num;

CREATE WINDOW VIEW window_view_metrics_view
(
    num UInt32,
    insertion_time DateTime,
    simulation_time DateTime64(9)
)
ENGINE = Memory WATERMARK toIntervalSecond(5)
AS SELECT
    window_view_metrics_src.num AS num,
    tumbleStart(window_id) AS insertion_time,
    max(window_view_metrics_src.simulation_time) AS simulation_time
FROM window_view_metrics_src
WHERE window_view_metrics_src.num != 0
GROUP BY
    window_view_metrics_src.num,
    tumble(CAST(toStartOfSecond(window_view_metrics_src.insertion_time), 'DateTime'), toIntervalSecond(1)) AS window_id;

SET log_comment = '04539_window_view';
INSERT INTO window_view_metrics_src VALUES
    (1, toDateTime64('2024-10-01 12:30:00', 9), toDateTime64('2024-10-01 12:00:00', 9));
SET log_comment = '';

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['DirectInsertedRows'],
    ProfileEvents['MaterializedViewInsertedRows']
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
