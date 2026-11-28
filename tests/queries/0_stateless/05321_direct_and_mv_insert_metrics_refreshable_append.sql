-- Tags: no-replicated-database
-- no-replicated-database: the refresh may run on another replica, and system.query_log is replica-local.

-- Checks the DirectInsertedRows and MaterializedViewInsertedRows profile events of an APPEND refresh
-- of a refreshable materialized view. The refresh is a separate `InterpreterInsertQuery` built in
-- `RefreshTask`: its rows written into the target table are Direct, and the rows written by the
-- materialized view that depends on the target table are MaterializedView.

DROP TABLE IF EXISTS rmv_metrics_rmv;
DROP TABLE IF EXISTS rmv_metrics_mv;
DROP TABLE IF EXISTS rmv_metrics_target;
DROP TABLE IF EXISTS rmv_metrics_downstream;

CREATE TABLE rmv_metrics_target (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE rmv_metrics_downstream (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE MATERIALIZED VIEW rmv_metrics_mv TO rmv_metrics_downstream AS SELECT x FROM rmv_metrics_target WHERE x % 2 = 0;

CREATE MATERIALIZED VIEW rmv_metrics_rmv REFRESH EVERY 1 YEAR APPEND TO rmv_metrics_target AS SELECT number AS x FROM numbers(5);
SYSTEM WAIT VIEW rmv_metrics_rmv;

SELECT count() FROM rmv_metrics_target;
SELECT count() FROM rmv_metrics_downstream;

SYSTEM FLUSH LOGS query_log;

-- The refresh query runs in the context of the view, so it is found by its log_comment, which contains
-- the database name, instead of by the condition current_database = currentDatabase().
-- 5 rows go directly into rmv_metrics_target and 3 of them (0, 2, 4) into rmv_metrics_downstream via the view.
SELECT
    ProfileEvents['DirectInsertedRows'],
    ProfileEvents['MaterializedViewInsertedRows'],
    ProfileEvents['DirectInsertedRows'] + ProfileEvents['MaterializedViewInsertedRows'] = ProfileEvents['InsertedRows'],
    ProfileEvents['DirectInsertedBytes'] + ProfileEvents['MaterializedViewInsertedBytes'] = ProfileEvents['InsertedBytes']
FROM system.query_log
WHERE log_comment LIKE 'refresh of ' || currentDatabase() || '.rmv_metrics_rmv%'
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
ORDER BY event_time DESC
LIMIT 1;

DROP TABLE rmv_metrics_rmv;
DROP TABLE rmv_metrics_mv;
DROP TABLE rmv_metrics_target;
DROP TABLE rmv_metrics_downstream;
