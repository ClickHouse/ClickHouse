-- Checks the DirectInsertedRows/DirectInsertedBytes and
-- MaterializedViewInsertedRows/MaterializedViewInsertedBytes profile events.
-- Rows written by the top-level INSERT are attributed to the "Direct" counters, while
-- rows written by a materialized view into its target table are attributed to the
-- "MaterializedView" counters. Per-query ProfileEvents from system.query_log (isolated
-- by current_database) make the check deterministic and independent of other queries
-- running on the server. The INSERT queries are tagged with an inline comment so they
-- can be found in system.query_log.

SET log_queries = 1;
SET log_queries_min_type = 'QUERY_FINISH';
SET parallel_view_processing = 0;

DROP TABLE IF EXISTS mv_metrics_src;
DROP TABLE IF EXISTS mv_metrics_dst;
DROP VIEW IF EXISTS mv_metrics_mv;

CREATE TABLE mv_metrics_src (id UInt64, s String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE mv_metrics_dst (id UInt64, s String) ENGINE = MergeTree ORDER BY id;
CREATE MATERIALIZED VIEW mv_metrics_mv TO mv_metrics_dst AS SELECT id, s FROM mv_metrics_src;

INSERT INTO /* test 04498 direct */ mv_metrics_dst SELECT number, toString(number) FROM numbers(5);
INSERT INTO /* test 04498 mv */ mv_metrics_src SELECT number, toString(number) FROM numbers(10);

SYSTEM FLUSH LOGS query_log;

-- Plain INSERT into a table without materialized views: all 5 rows are Direct, none are MaterializedView.
SELECT
    ProfileEvents['DirectInsertedRows'],
    ProfileEvents['MaterializedViewInsertedRows']
FROM system.query_log
WHERE current_database = currentDatabase()
  AND query LIKE 'INSERT INTO /* test 04498 direct */%'
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
ORDER BY event_time DESC
LIMIT 1;

-- INSERT feeding a materialized view: 10 rows go directly into mv_metrics_src and 10 rows into mv_metrics_dst via the view.
SELECT
    ProfileEvents['DirectInsertedRows'],
    ProfileEvents['MaterializedViewInsertedRows']
FROM system.query_log
WHERE current_database = currentDatabase()
  AND query LIKE 'INSERT INTO /* test 04498 mv */%'
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
ORDER BY event_time DESC
LIMIT 1;

-- Both byte counters must be non-zero for the materialized-view INSERT.
SELECT
    ProfileEvents['DirectInsertedBytes'] > 0,
    ProfileEvents['MaterializedViewInsertedBytes'] > 0
FROM system.query_log
WHERE current_database = currentDatabase()
  AND query LIKE 'INSERT INTO /* test 04498 mv */%'
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
ORDER BY event_time DESC
LIMIT 1;

-- The Direct and MaterializedView counters together must account for all InsertedRows/InsertedBytes.
SELECT
    ProfileEvents['DirectInsertedRows'] + ProfileEvents['MaterializedViewInsertedRows'] = ProfileEvents['InsertedRows'],
    ProfileEvents['DirectInsertedBytes'] + ProfileEvents['MaterializedViewInsertedBytes'] = ProfileEvents['InsertedBytes']
FROM system.query_log
WHERE current_database = currentDatabase()
  AND query LIKE 'INSERT INTO /* test 04498 mv */%'
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
ORDER BY event_time DESC
LIMIT 1;

DROP VIEW mv_metrics_mv;
DROP TABLE mv_metrics_src;
DROP TABLE mv_metrics_dst;
