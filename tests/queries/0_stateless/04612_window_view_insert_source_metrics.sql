-- Rows entering a window view must not be attributed to the
-- MaterializedViewInsertedRows/MaterializedViewInsertedBytes profile events: those events
-- count only pushes from insert-triggered materialized views into their target tables.
-- Per-query ProfileEvents from system.query_log (isolated by current_database) make the
-- check deterministic. The INSERT query is tagged with an inline comment so it can be
-- found in system.query_log.

SET allow_experimental_window_view = 1;
SET allow_experimental_analyzer = 0;
SET log_queries = 1;
SET log_queries_min_type = 'QUERY_FINISH';

DROP TABLE IF EXISTS wv_metrics_src;
DROP TABLE IF EXISTS wv_metrics_wv;

CREATE TABLE wv_metrics_src (id UInt64, ts DateTime) ENGINE = MergeTree ORDER BY id;
CREATE WINDOW VIEW wv_metrics_wv ENGINE Memory AS
    SELECT count(id) AS cnt, tumbleStart(w_id) AS w_start
    FROM wv_metrics_src
    GROUP BY tumble(ts, INTERVAL '5' SECOND) AS w_id;

INSERT INTO /* test 04612 window view */ wv_metrics_src SELECT number, toDateTime('2026-01-01 00:00:00') + number FROM numbers(7);

SYSTEM FLUSH LOGS query_log;

-- All 7 rows written into wv_metrics_src are Direct; the rows entering the window view
-- must not bump the MaterializedView counters.
SELECT
    ProfileEvents['DirectInsertedRows'],
    ProfileEvents['MaterializedViewInsertedRows'],
    ProfileEvents['MaterializedViewInsertedBytes']
FROM system.query_log
WHERE current_database = currentDatabase()
  AND query LIKE 'INSERT INTO /* test 04612 window view */%'
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
ORDER BY event_time DESC
LIMIT 1;

DROP TABLE wv_metrics_wv;
DROP TABLE wv_metrics_src;
