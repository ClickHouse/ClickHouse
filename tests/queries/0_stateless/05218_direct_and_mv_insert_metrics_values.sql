-- Checks the DirectInsertedRows/DirectInsertedBytes and
-- MaterializedViewInsertedRows/MaterializedViewInsertedBytes profile events for plain
-- `INSERT ... VALUES` queries. Unlike `INSERT ... SELECT`, which is built by
-- `InterpreterInsertQuery::addInsertToSelectPipeline`, inline data goes through
-- `InterpreterInsertQuery::buildInsertPipeline`, so the split counters must be attributed
-- correctly on that path as well: rows written by the INSERT itself are Direct and rows written
-- by a materialized view into its target are MaterializedView. Per-query ProfileEvents from
-- system.query_log (isolated by current_database) make the check deterministic. The INSERT
-- queries are tagged with an inline comment so they can be found in system.query_log.

SET log_queries = 1;
SET log_queries_min_type = 'QUERY_FINISH';
SET parallel_view_processing = 0;

DROP TABLE IF EXISTS values_metrics_src;
DROP TABLE IF EXISTS values_metrics_dst;
DROP VIEW IF EXISTS values_metrics_mv;

CREATE TABLE values_metrics_src (id UInt64, s String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE values_metrics_dst (id UInt64, s String) ENGINE = MergeTree ORDER BY id;
CREATE MATERIALIZED VIEW values_metrics_mv TO values_metrics_dst AS SELECT id, s FROM values_metrics_src;

INSERT INTO /* test 05218 direct */ values_metrics_dst VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e');
INSERT INTO /* test 05218 mv */ values_metrics_src VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd');
INSERT INTO /* test 05218 function */ FUNCTION null('id UInt64, s String') VALUES (1, 'a'), (2, 'b'), (3, 'c');

SYSTEM FLUSH LOGS query_log;

-- Plain INSERT ... VALUES into a table without materialized views: all 5 rows are Direct.
SELECT
    ProfileEvents['DirectInsertedRows'],
    ProfileEvents['MaterializedViewInsertedRows'],
    ProfileEvents['DirectInsertedBytes'] > 0,
    ProfileEvents['MaterializedViewInsertedBytes'],
    ProfileEvents['DirectInsertedRows'] + ProfileEvents['MaterializedViewInsertedRows'] = ProfileEvents['InsertedRows'],
    ProfileEvents['DirectInsertedBytes'] + ProfileEvents['MaterializedViewInsertedBytes'] = ProfileEvents['InsertedBytes']
FROM system.query_log
WHERE current_database = currentDatabase()
  AND query LIKE 'INSERT INTO /* test 05218 direct */%'
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
ORDER BY event_time DESC
LIMIT 1;

-- INSERT ... VALUES feeding a materialized view: 4 rows go directly into values_metrics_src and
-- 4 rows into values_metrics_dst via the view.
SELECT
    ProfileEvents['DirectInsertedRows'],
    ProfileEvents['MaterializedViewInsertedRows'],
    ProfileEvents['DirectInsertedBytes'] > 0,
    ProfileEvents['MaterializedViewInsertedBytes'] > 0,
    ProfileEvents['DirectInsertedRows'] + ProfileEvents['MaterializedViewInsertedRows'] = ProfileEvents['InsertedRows'],
    ProfileEvents['DirectInsertedBytes'] + ProfileEvents['MaterializedViewInsertedBytes'] = ProfileEvents['InsertedBytes']
FROM system.query_log
WHERE current_database = currentDatabase()
  AND query LIKE 'INSERT INTO /* test 05218 mv */%'
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
ORDER BY event_time DESC
LIMIT 1;

-- INSERT ... VALUES into the sink of a table function: the 3 rows are Direct as well.
SELECT
    ProfileEvents['DirectInsertedRows'],
    ProfileEvents['MaterializedViewInsertedRows'],
    ProfileEvents['DirectInsertedBytes'] > 0,
    ProfileEvents['MaterializedViewInsertedBytes'],
    ProfileEvents['DirectInsertedRows'] + ProfileEvents['MaterializedViewInsertedRows'] = ProfileEvents['InsertedRows'],
    ProfileEvents['DirectInsertedBytes'] + ProfileEvents['MaterializedViewInsertedBytes'] = ProfileEvents['InsertedBytes']
FROM system.query_log
WHERE current_database = currentDatabase()
  AND query LIKE 'INSERT INTO /* test 05218 function */%'
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
ORDER BY event_time DESC
LIMIT 1;

DROP VIEW values_metrics_mv;
DROP TABLE values_metrics_src;
DROP TABLE values_metrics_dst;
