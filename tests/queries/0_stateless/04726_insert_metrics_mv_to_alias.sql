-- Preserve materialized-view insert attribution when an `Alias` target starts a nested `INSERT`.

SET log_queries = 1;
SET log_queries_min_type = 'QUERY_FINISH';
SET parallel_view_processing = 0;

DROP VIEW IF EXISTS insert_metrics_mv_to_alias_mv;
DROP TABLE IF EXISTS insert_metrics_mv_to_alias_alias;
DROP TABLE IF EXISTS insert_metrics_mv_to_alias_dst;
DROP TABLE IF EXISTS insert_metrics_mv_to_alias_src;

CREATE TABLE insert_metrics_mv_to_alias_src (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE insert_metrics_mv_to_alias_dst (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE insert_metrics_mv_to_alias_alias ENGINE = Alias('insert_metrics_mv_to_alias_dst');
CREATE MATERIALIZED VIEW insert_metrics_mv_to_alias_mv TO insert_metrics_mv_to_alias_alias AS
    SELECT id FROM insert_metrics_mv_to_alias_src WHERE id < 5;

SET log_comment = '04726_mv_to_alias';
INSERT INTO insert_metrics_mv_to_alias_src SELECT number FROM numbers(10);
SET log_comment = '';

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['DirectInsertedRows'],
    ProfileEvents['MaterializedViewInsertedRows'],
    ProfileEvents['InsertedRows'],
    ProfileEvents['DirectInsertedRows'] + ProfileEvents['MaterializedViewInsertedRows'] = ProfileEvents['InsertedRows'],
    ProfileEvents['DirectInsertedBytes'] + ProfileEvents['MaterializedViewInsertedBytes'] = ProfileEvents['InsertedBytes']
FROM system.query_log
WHERE current_database = currentDatabase()
  AND log_comment = '04726_mv_to_alias'
  AND query LIKE 'INSERT INTO insert_metrics_mv_to_alias_src%'
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
ORDER BY event_time DESC
LIMIT 1;

SELECT count() FROM insert_metrics_mv_to_alias_dst;

DROP VIEW insert_metrics_mv_to_alias_mv;
DROP TABLE insert_metrics_mv_to_alias_alias;
DROP TABLE insert_metrics_mv_to_alias_dst;
DROP TABLE insert_metrics_mv_to_alias_src;
