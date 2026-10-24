-- Checks root INSERT and dependent materialized-view insert profile events.
-- The INSERTs are selected by log_comment from system.query_log after flushing logs, which
-- keeps this test independent of concurrent queries on the server.

SET log_queries = 1;
SET log_queries_min_type = 'QUERY_FINISH';
SET parallel_view_processing = 0;
SET async_insert = 0;

DROP VIEW IF EXISTS mv_metrics_chain_2_mv;
DROP VIEW IF EXISTS mv_metrics_chain_1_mv;
DROP VIEW IF EXISTS mv_metrics_zero_mv;
DROP VIEW IF EXISTS mv_metrics_fanout_2_mv;
DROP VIEW IF EXISTS mv_metrics_fanout_1_mv;
DROP VIEW IF EXISTS mv_metrics_filter_mv;
DROP TABLE IF EXISTS mv_metrics_chain_2;
DROP TABLE IF EXISTS mv_metrics_chain_1;
DROP TABLE IF EXISTS mv_metrics_zero;
DROP TABLE IF EXISTS mv_metrics_fanout_2;
DROP TABLE IF EXISTS mv_metrics_fanout_1;
DROP TABLE IF EXISTS mv_metrics_filter;
DROP TABLE IF EXISTS mv_metrics_src;
DROP TABLE IF EXISTS mv_metrics_direct;

CREATE TABLE mv_metrics_direct (id UInt64, s String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE mv_metrics_src (id UInt64, s String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE mv_metrics_filter (id UInt64, s String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE mv_metrics_fanout_1 (id UInt64, s String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE mv_metrics_fanout_2 (id UInt64, s String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE mv_metrics_zero (id UInt64, s String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE mv_metrics_chain_1 (id UInt64, s String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE mv_metrics_chain_2 (id UInt64, s String) ENGINE = MergeTree ORDER BY id;

CREATE MATERIALIZED VIEW mv_metrics_filter_mv TO mv_metrics_filter AS
    SELECT id, s FROM mv_metrics_src WHERE id < 3;
CREATE MATERIALIZED VIEW mv_metrics_fanout_1_mv TO mv_metrics_fanout_1 AS
    SELECT id, s FROM mv_metrics_src WHERE id < 2;
CREATE MATERIALIZED VIEW mv_metrics_fanout_2_mv TO mv_metrics_fanout_2 AS
    SELECT id, s FROM mv_metrics_src WHERE id < 4;
CREATE MATERIALIZED VIEW mv_metrics_zero_mv TO mv_metrics_zero AS
    SELECT id, s FROM mv_metrics_src WHERE false;
CREATE MATERIALIZED VIEW mv_metrics_chain_1_mv TO mv_metrics_chain_1 AS
    SELECT id, s FROM mv_metrics_src;
CREATE MATERIALIZED VIEW mv_metrics_chain_2_mv TO mv_metrics_chain_2 AS
    SELECT id, s FROM mv_metrics_chain_1 WHERE id < 4;

-- Inline data takes the buildInsertPipeline path.
SET log_comment = '04538_direct';
INSERT INTO mv_metrics_direct VALUES (0, '0'), (1, '1'), (2, '2'), (3, '3'), (4, '4');
-- INSERT SELECT takes the addInsertToSelectPipeline path. The views produce
-- 3 + 2 + 4 + 0 + 10 + 4 = 23 rows, including a fan-out and a chain.
SET log_comment = '04538_views';
INSERT INTO mv_metrics_src SELECT number, toString(number) FROM numbers(10);
SET log_comment = '';

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['DirectInsertedRows'],
    ProfileEvents['MaterializedViewInsertedRows'],
    ProfileEvents['DirectInsertedBytes'] = ProfileEvents['InsertedBytes'],
    ProfileEvents['MaterializedViewInsertedBytes'] = 0
FROM system.query_log
WHERE current_database = currentDatabase()
  AND log_comment = '04538_direct'
  AND query LIKE 'INSERT INTO mv_metrics_direct%'
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
ORDER BY event_time DESC
LIMIT 1;

SELECT
    ProfileEvents['DirectInsertedRows'],
    ProfileEvents['MaterializedViewInsertedRows'],
    ProfileEvents['InsertedRows'],
    ProfileEvents['DirectInsertedRows'] + ProfileEvents['MaterializedViewInsertedRows'] = ProfileEvents['InsertedRows']
FROM system.query_log
WHERE current_database = currentDatabase()
  AND log_comment = '04538_views'
  AND query LIKE 'INSERT INTO mv_metrics_src%'
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
ORDER BY event_time DESC
LIMIT 1;

SELECT
    ProfileEvents['DirectInsertedBytes'] > 0,
    ProfileEvents['MaterializedViewInsertedBytes'] > 0,
    ProfileEvents['DirectInsertedBytes'] + ProfileEvents['MaterializedViewInsertedBytes'] = ProfileEvents['InsertedBytes']
FROM system.query_log
WHERE current_database = currentDatabase()
  AND log_comment = '04538_views'
  AND query LIKE 'INSERT INTO mv_metrics_src%'
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
ORDER BY event_time DESC
LIMIT 1;

SELECT count() FROM mv_metrics_filter;
SELECT count() FROM mv_metrics_fanout_1;
SELECT count() FROM mv_metrics_fanout_2;
SELECT count() FROM mv_metrics_zero;
SELECT count() FROM mv_metrics_chain_1;
SELECT count() FROM mv_metrics_chain_2;

DROP VIEW mv_metrics_chain_2_mv;
DROP VIEW mv_metrics_chain_1_mv;
DROP VIEW mv_metrics_zero_mv;
DROP VIEW mv_metrics_fanout_2_mv;
DROP VIEW mv_metrics_fanout_1_mv;
DROP VIEW mv_metrics_filter_mv;
DROP TABLE mv_metrics_chain_2;
DROP TABLE mv_metrics_chain_1;
DROP TABLE mv_metrics_zero;
DROP TABLE mv_metrics_fanout_2;
DROP TABLE mv_metrics_fanout_1;
DROP TABLE mv_metrics_filter;
DROP TABLE mv_metrics_src;
DROP TABLE mv_metrics_direct;
