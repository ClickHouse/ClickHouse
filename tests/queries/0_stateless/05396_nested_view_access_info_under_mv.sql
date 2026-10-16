-- An INSERT-only caller reaches a protected lookup through a DEFINER materialized view and a nested DEFINER view.
-- The outer insert keeps complete operator attribution, while its own query-log row hides all access arrays.
SET log_queries = 1;
SET log_queries_probability = 1;
SET log_queries_min_type = 'QUERY_START';
SET analyzer_inline_views = 0;

DROP USER IF EXISTS user_05396_mv_caller;
DROP TABLE IF EXISTS mv_05396_nested;
DROP VIEW IF EXISTS v_05396_protected;
DROP TABLE IF EXISTS t_05396_sink;
DROP TABLE IF EXISTS t_05396_source;
DROP TABLE IF EXISTS t_05396_protected;

CREATE TABLE t_05396_protected (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_05396_protected VALUES (1);

CREATE VIEW v_05396_protected DEFINER = CURRENT_USER SQL SECURITY DEFINER AS
SELECT id FROM t_05396_protected;

CREATE TABLE t_05396_source (id UInt64) ENGINE = Null;
CREATE TABLE t_05396_sink (id UInt64) ENGINE = MergeTree ORDER BY id;

CREATE MATERIALIZED VIEW mv_05396_nested TO t_05396_sink
DEFINER = CURRENT_USER SQL SECURITY DEFINER
AS SELECT id FROM t_05396_source WHERE id IN (SELECT id FROM v_05396_protected);

CREATE USER user_05396_mv_caller;
GRANT INSERT ON t_05396_source TO user_05396_mv_caller;
-- This run marker gives the impersonated reader a fresh lower bound without granting query-log access.
CREATE TABLE t_05396_run_marker (started_at DateTime64(6, 'UTC')) ENGINE = Memory;
INSERT INTO t_05396_run_marker SELECT now64(6, 'UTC');
GRANT SELECT ON t_05396_run_marker TO user_05396_mv_caller;

EXECUTE AS user_05396_mv_caller INSERT INTO t_05396_source SETTINGS log_comment = '05396_nested_mv_insert' VALUES (1);

SYSTEM FLUSH LOGS query_log;

-- The materialized-view trigger ran its protected lookup and shared attribution back to the outer INSERT.
SELECT count() = 1,
    countIf(query_access_info_is_hidden = 1
        AND has(tables, concat(currentDatabase(), '.t_05396_source'))
        AND has(tables, concat(currentDatabase(), '.t_05396_protected'))) = 1
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND initial_query_id = query_id
    AND query_start_time_microseconds >= (SELECT started_at FROM t_05396_run_marker)
    AND initial_user = 'user_05396_mv_caller'
    AND log_comment = '05396_nested_mv_insert';

SELECT count() FROM t_05396_sink;

-- The INSERT-only caller's own row is identified by initial_user and the fresh run marker, with all seven arrays masked.
EXECUTE AS user_05396_mv_caller SELECT count() = 1,
    countIf(initial_user = 'user_05396_mv_caller') = 1,
    countIf(notEmpty(databases) OR notEmpty(tables) OR notEmpty(columns) OR notEmpty(partitions)
        OR notEmpty(projections) OR notEmpty(views) OR notEmpty(used_row_policies)) = 0
FROM system.user_query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND initial_query_id = query_id
    AND query_start_time_microseconds >= (SELECT started_at FROM t_05396_run_marker)
    AND initial_user = 'user_05396_mv_caller'
    AND log_comment = '05396_nested_mv_insert';

DROP USER user_05396_mv_caller;
DROP TABLE mv_05396_nested;
DROP VIEW v_05396_protected;
DROP TABLE t_05396_sink;
DROP TABLE t_05396_source;
DROP TABLE t_05396_protected;
DROP TABLE t_05396_run_marker;
