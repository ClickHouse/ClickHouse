-- `InsertDependenciesBuilder::createSelect` is shared between materialized views and window
-- views, so it classifies the insert source from the view type: only `MATERIALIZED` bumps
-- `MaterializedViewInsertedRows`/`MaterializedViewInsertedBytes`, a window view falls into
-- `InsertSource::Other` and bumps only the generic `InsertedRows`/`InsertedBytes`.
--
-- Today a window view is never actually part of an insert chain: it registers no view
-- dependency on its source table (unlike a materialized view), and a direct
-- `INSERT INTO <window view>` fails before the chain is built. So this test pins BOTH halves
-- of that state instead of only asserting that the materialized-view counters stay zero -
-- zeros alone would also hold if the window view silently dropped out of the picture:
--   * the profile events of the tagged INSERT, and
--   * the fact that the window view is not an insert dependency of its source table.
-- If window views ever become part of the insert chain, the second check fails and the
-- attribution above has to be re-verified with a chain that really runs.
--
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

-- The window view is not wired into the insert chain of its source table, so nothing of the
-- INSERT below can reach the window-view branch. Expected: 0.
SELECT countIf(has(dependencies_table, 'wv_metrics_wv'))
FROM system.tables
WHERE database = currentDatabase();

INSERT INTO /* test 04612 window view */ wv_metrics_src SELECT number, toDateTime('2026-01-01 00:00:00') + number FROM numbers(7);

SYSTEM FLUSH LOGS query_log;

-- All 7 rows written into wv_metrics_src are Direct, the MaterializedView counters stay zero,
-- and the totals match the Direct counters exactly - nothing else contributed to this INSERT.
SELECT
    ProfileEvents['DirectInsertedRows'],
    ProfileEvents['MaterializedViewInsertedRows'],
    ProfileEvents['MaterializedViewInsertedBytes'],
    ProfileEvents['InsertedRows'] = ProfileEvents['DirectInsertedRows'],
    ProfileEvents['InsertedBytes'] = ProfileEvents['DirectInsertedBytes']
FROM system.query_log
WHERE current_database = currentDatabase()
  AND query LIKE 'INSERT INTO /* test 04612 window view */%'
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
ORDER BY event_time DESC
LIMIT 1;

DROP TABLE wv_metrics_wv;
DROP TABLE wv_metrics_src;
