-- Tags: no-parallel
-- Tag no-parallel: copies share global users with fixed names.

-- An INSERT-only caller reaches a protected lookup through a DEFINER materialized view and a nested DEFINER view.
-- The outer insert keeps complete operator attribution; its marked initial QueryFinish row is omitted from user_query_log.
SET log_queries = 1;
SET log_queries_probability = 1;
SET log_queries_min_type = 'QUERY_START';
SET analyzer_inline_views = 0;
SET use_query_cache = 0;

DROP USER IF EXISTS user_05396_mv_caller;
DROP USER IF EXISTS user_05396_root_mv_caller;
DROP USER IF EXISTS user_05396_invoker;
DROP TABLE IF EXISTS mv_05396_nested;
DROP TABLE IF EXISTS mv_05396_direct_definer;
DROP TABLE IF EXISTS mv_05396_direct_none;
DROP TABLE IF EXISTS mv_05396_multi_definer;
DROP TABLE IF EXISTS mv_05396_multi_none;
DROP TABLE IF EXISTS mv_05396_root_definer;
DROP TABLE IF EXISTS mv_05396_root_none;
DROP VIEW IF EXISTS v_05396_protected;
DROP VIEW IF EXISTS v_05396_invoker;
DROP TABLE IF EXISTS t_05396_invoker_expected_qai;
DROP TABLE IF EXISTS t_05396_invoker_base;
DROP TABLE IF EXISTS t_05396_sink;
DROP TABLE IF EXISTS t_05396_direct_definer_sink;
DROP TABLE IF EXISTS t_05396_direct_none_sink;
DROP TABLE IF EXISTS t_05396_multi_none_sink;
DROP TABLE IF EXISTS t_05396_multi_sink;
DROP TABLE IF EXISTS t_05396_root_definer_sink;
DROP TABLE IF EXISTS t_05396_root_none_sink;
DROP TABLE IF EXISTS t_05396_root_protected;
DROP TABLE IF EXISTS t_05396_direct_definer_source;
DROP TABLE IF EXISTS t_05396_direct_none_source;
DROP TABLE IF EXISTS t_05396_multi_source;
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

-- These dependent MVs read the protected table directly, without an ordinary StorageView marker.
-- Each has a private source so no other triggered MV can set the marker on its behalf.
CREATE TABLE t_05396_direct_definer_source (id UInt64) ENGINE = Null;
CREATE TABLE t_05396_direct_none_source (id UInt64) ENGINE = Null;
CREATE TABLE t_05396_direct_definer_sink (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_05396_direct_none_sink (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE MATERIALIZED VIEW mv_05396_direct_definer TO t_05396_direct_definer_sink
DEFINER = CURRENT_USER SQL SECURITY DEFINER
AS SELECT id FROM t_05396_direct_definer_source WHERE id IN (SELECT id FROM t_05396_protected);
CREATE MATERIALIZED VIEW mv_05396_direct_none TO t_05396_direct_none_sink
DEFINER = CURRENT_USER SQL SECURITY NONE
AS SELECT id FROM t_05396_direct_none_source WHERE id IN (SELECT id FROM t_05396_protected);

-- This dependent MV follows an INVOKER view while the other MVs mark the same insert hidden.
CREATE TABLE t_05396_invoker_base (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_05396_invoker_base VALUES (1);
CREATE VIEW v_05396_invoker SQL SECURITY INVOKER AS SELECT id FROM t_05396_invoker_base;
CREATE TABLE t_05396_multi_source (id UInt64) ENGINE = Null;
CREATE TABLE t_05396_multi_sink (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_05396_multi_none_sink (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE MATERIALIZED VIEW mv_05396_multi_definer TO t_05396_multi_sink
DEFINER = CURRENT_USER SQL SECURITY DEFINER
AS SELECT id FROM t_05396_multi_source WHERE id IN (SELECT id FROM v_05396_invoker);
CREATE MATERIALIZED VIEW mv_05396_multi_none TO t_05396_multi_none_sink
DEFINER = CURRENT_USER SQL SECURITY NONE
AS SELECT id FROM t_05396_multi_source WHERE id IN (SELECT id FROM t_05396_protected);

-- Direct root-target inserts exercise the root-view context setup separately from source triggers.
CREATE TABLE t_05396_root_protected (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_05396_root_definer_sink (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_05396_root_none_sink (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE MATERIALIZED VIEW mv_05396_root_definer TO t_05396_root_definer_sink
DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT id FROM t_05396_root_protected;
CREATE MATERIALIZED VIEW mv_05396_root_none TO t_05396_root_none_sink
DEFINER = CURRENT_USER SQL SECURITY NONE AS SELECT id FROM t_05396_root_protected;

CREATE USER user_05396_mv_caller;
GRANT INSERT ON t_05396_source TO user_05396_mv_caller;
GRANT INSERT ON t_05396_direct_definer_source TO user_05396_mv_caller;
GRANT INSERT ON t_05396_direct_none_source TO user_05396_mv_caller;
GRANT INSERT ON t_05396_multi_source TO user_05396_mv_caller;
-- This run marker gives the impersonated reader a fresh lower bound without granting query-log access.
CREATE TABLE t_05396_run_marker (started_at DateTime64(6, 'UTC')) ENGINE = Memory;
INSERT INTO t_05396_run_marker SELECT now64(6, 'UTC');
GRANT SELECT ON t_05396_run_marker TO user_05396_mv_caller;

CREATE USER user_05396_root_mv_caller;
GRANT INSERT ON mv_05396_root_definer TO user_05396_root_mv_caller;
GRANT INSERT ON mv_05396_root_none TO user_05396_root_mv_caller;
GRANT INSERT ON t_05396_root_protected TO user_05396_root_mv_caller;
GRANT SELECT ON t_05396_run_marker TO user_05396_root_mv_caller;

CREATE USER user_05396_invoker;
GRANT SELECT ON t_05396_invoker_base TO user_05396_invoker;
GRANT SELECT ON v_05396_invoker TO user_05396_invoker;
CREATE TABLE t_05396_invoker_expected_qai
(
    query_id String,
    databases Array(String),
    tables Array(String),
    columns Array(String),
    partitions Array(String),
    projections Array(String),
    views Array(String),
    used_row_policies Array(String)
)
ENGINE = Memory;
GRANT SELECT ON t_05396_invoker_expected_qai TO user_05396_invoker;
GRANT SELECT ON t_05396_run_marker TO user_05396_invoker;

EXECUTE AS user_05396_mv_caller INSERT INTO t_05396_source SETTINGS log_comment = '05396_nested_mv_insert' SELECT toUInt64(1);

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

-- The INSERT-only caller cannot see the marked initial QueryFinish row.
EXECUTE AS user_05396_mv_caller SELECT count() = 0,
    countIf(initial_user = 'user_05396_mv_caller') = 0,
    countIf(notEmpty(databases) OR notEmpty(tables) OR notEmpty(columns) OR notEmpty(partitions)
        OR notEmpty(projections) OR notEmpty(views) OR notEmpty(used_row_policies)) = 0
FROM system.user_query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND initial_query_id = query_id
    AND query_start_time_microseconds >= (SELECT started_at FROM t_05396_run_marker)
    AND initial_user = 'user_05396_mv_caller'
    AND log_comment = '05396_nested_mv_insert';

-- Report only booleans for whether the restricted source writer already sees protected names in system.tables.
EXECUTE AS user_05396_mv_caller SELECT
    countIf(name = 't_05396_protected') = 0,
    countIf(name = 'v_05396_protected') = 0,
    countIf(name IN ('mv_05396_nested', 'mv_05396_direct_definer', 'mv_05396_direct_none',
        'mv_05396_multi_definer', 'mv_05396_multi_none')) = 0
FROM system.tables
WHERE database = currentDatabase()
    AND name IN ('t_05396_protected', 'v_05396_protected', 'mv_05396_nested', 'mv_05396_direct_definer',
        'mv_05396_direct_none', 'mv_05396_multi_definer', 'mv_05396_multi_none');

-- Isolated source inserts prove each direct DEFINER/NONE MV sets the marker itself.
EXECUTE AS user_05396_mv_caller INSERT INTO t_05396_direct_definer_source
SETTINGS log_comment = '05396_direct_definer_insert' SELECT toUInt64(1);
EXECUTE AS user_05396_mv_caller INSERT INTO t_05396_direct_none_source
SETTINGS log_comment = '05396_direct_none_insert' SELECT toUInt64(1);
SYSTEM FLUSH LOGS query_log;

SELECT count() = 1,
    countIf(query_access_info_is_hidden = 1
        AND has(tables, concat(currentDatabase(), '.t_05396_direct_definer_source'))
        AND has(tables, concat(currentDatabase(), '.t_05396_protected'))
        AND has(views, concat(currentDatabase(), '.mv_05396_direct_definer'))) = 1
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND initial_query_id = query_id
    AND query_start_time_microseconds >= (SELECT started_at FROM t_05396_run_marker)
    AND initial_user = 'user_05396_mv_caller'
    AND log_comment = '05396_direct_definer_insert';

EXECUTE AS user_05396_mv_caller SELECT count() = 0,
    countIf(notEmpty(databases) OR notEmpty(tables) OR notEmpty(columns) OR notEmpty(partitions)
        OR notEmpty(projections) OR notEmpty(views) OR notEmpty(used_row_policies)) = 0
FROM system.user_query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND initial_query_id = query_id
    AND query_start_time_microseconds >= (SELECT started_at FROM t_05396_run_marker)
    AND initial_user = 'user_05396_mv_caller'
    AND log_comment = '05396_direct_definer_insert';

SELECT count() FROM t_05396_direct_definer_sink;

SELECT count() = 1,
    countIf(query_access_info_is_hidden = 1
        AND has(tables, concat(currentDatabase(), '.t_05396_direct_none_source'))
        AND has(tables, concat(currentDatabase(), '.t_05396_protected'))
        AND has(views, concat(currentDatabase(), '.mv_05396_direct_none'))) = 1
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND initial_query_id = query_id
    AND query_start_time_microseconds >= (SELECT started_at FROM t_05396_run_marker)
    AND initial_user = 'user_05396_mv_caller'
    AND log_comment = '05396_direct_none_insert';

EXECUTE AS user_05396_mv_caller SELECT count() = 0,
    countIf(notEmpty(databases) OR notEmpty(tables) OR notEmpty(columns) OR notEmpty(partitions)
        OR notEmpty(projections) OR notEmpty(views) OR notEmpty(used_row_policies)) = 0
FROM system.user_query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND initial_query_id = query_id
    AND query_start_time_microseconds >= (SELECT started_at FROM t_05396_run_marker)
    AND initial_user = 'user_05396_mv_caller'
    AND log_comment = '05396_direct_none_insert';

SELECT count() FROM t_05396_direct_none_sink;

-- Two MVs include an INVOKER-view path and an independent NONE boundary. The latter must mark
-- the canonical query while the operator log retains all discovered dependencies.
EXECUTE AS user_05396_mv_caller INSERT INTO t_05396_multi_source
SETTINGS log_comment = '05396_multi_mv_insert' SELECT toUInt64(1);
SYSTEM FLUSH LOGS query_log;

SELECT count() = 1,
    countIf(query_access_info_is_hidden = 1
        AND has(tables, concat(currentDatabase(), '.t_05396_multi_source'))
        AND has(tables, concat(currentDatabase(), '.t_05396_invoker_base'))
        AND has(tables, concat(currentDatabase(), '.t_05396_protected'))
        AND has(views, concat(currentDatabase(), '.mv_05396_multi_definer'))
        AND has(views, concat(currentDatabase(), '.mv_05396_multi_none'))) = 1
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND initial_query_id = query_id
    AND query_start_time_microseconds >= (SELECT started_at FROM t_05396_run_marker)
    AND initial_user = 'user_05396_mv_caller'
    AND log_comment = '05396_multi_mv_insert';

EXECUTE AS user_05396_mv_caller SELECT count() = 0,
    countIf(notEmpty(databases) OR notEmpty(tables) OR notEmpty(columns) OR notEmpty(partitions)
        OR notEmpty(projections) OR notEmpty(views) OR notEmpty(used_row_policies)) = 0
FROM system.user_query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND initial_query_id = query_id
    AND query_start_time_microseconds >= (SELECT started_at FROM t_05396_run_marker)
    AND initial_user = 'user_05396_mv_caller'
    AND log_comment = '05396_multi_mv_insert';

SELECT (SELECT count() FROM t_05396_multi_sink), (SELECT count() FROM t_05396_multi_none_sink);

-- INVOKER alone does not set the marker; its caller-visible arrays match the operator row exactly.
EXECUTE AS user_05396_invoker SELECT id FROM v_05396_invoker WHERE id = 1
FORMAT Null SETTINGS log_comment = '05396_invoker_control';
SYSTEM FLUSH LOGS query_log;
INSERT INTO t_05396_invoker_expected_qai
SELECT query_id, databases, tables, columns, partitions, projections, views, used_row_policies
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND initial_query_id = query_id
    AND initial_user = 'user_05396_invoker'
    AND query_start_time_microseconds >= (SELECT started_at FROM t_05396_run_marker)
    AND log_comment = '05396_invoker_control';

SELECT count() = 1,
    countIf(query_access_info_is_hidden = 0
        AND has(tables, concat(currentDatabase(), '.t_05396_invoker_base'))) = 1
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND initial_query_id = query_id
    AND initial_user = 'user_05396_invoker'
    AND query_start_time_microseconds >= (SELECT started_at FROM t_05396_run_marker)
    AND log_comment = '05396_invoker_control';

EXECUTE AS user_05396_invoker SELECT
    count() = 1,
    uniqExact(caller.query_id) = 1,
    countIf(caller.databases = expected.databases
        AND caller.tables = expected.tables
        AND caller.columns = expected.columns
        AND caller.partitions = expected.partitions
        AND caller.projections = expected.projections
        AND caller.views = expected.views
        AND caller.used_row_policies = expected.used_row_policies) = 1,
    (SELECT count() = 1 AND uniqExact(query_id) = 1 FROM t_05396_invoker_expected_qai)
FROM system.user_query_log AS caller
INNER JOIN t_05396_invoker_expected_qai AS expected ON caller.query_id = expected.query_id
WHERE caller.current_database = currentDatabase()
    AND caller.type = 'QueryFinish'
    AND caller.initial_query_id = caller.query_id
    AND caller.initial_user = 'user_05396_invoker'
    AND caller.query_start_time_microseconds >= (SELECT started_at FROM t_05396_run_marker)
    AND caller.log_comment = '05396_invoker_control';

-- These direct INSERTs test root-target DEFINER/NONE boundaries; their SELECT definitions may not run here.
EXECUTE AS user_05396_root_mv_caller INSERT INTO mv_05396_root_definer
SETTINGS log_comment = '05396_root_definer_insert' SELECT toUInt64(2);
EXECUTE AS user_05396_root_mv_caller INSERT INTO mv_05396_root_none
SETTINGS log_comment = '05396_root_none_insert' SELECT toUInt64(3);
SYSTEM FLUSH LOGS query_log;

SELECT count() = 1,
    countIf(query_access_info_is_hidden = 1) = 1
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND initial_query_id = query_id
    AND initial_user = 'user_05396_root_mv_caller'
    AND query_start_time_microseconds >= (SELECT started_at FROM t_05396_run_marker)
    AND log_comment = '05396_root_definer_insert';

EXECUTE AS user_05396_root_mv_caller SELECT count() = 0,
    countIf(notEmpty(databases) OR notEmpty(tables) OR notEmpty(columns) OR notEmpty(partitions)
        OR notEmpty(projections) OR notEmpty(views) OR notEmpty(used_row_policies)) = 0
FROM system.user_query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND initial_query_id = query_id
    AND initial_user = 'user_05396_root_mv_caller'
    AND query_start_time_microseconds >= (SELECT started_at FROM t_05396_run_marker)
    AND log_comment = '05396_root_definer_insert';

SELECT count() = 1,
    countIf(query_access_info_is_hidden = 1) = 1
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND initial_query_id = query_id
    AND initial_user = 'user_05396_root_mv_caller'
    AND query_start_time_microseconds >= (SELECT started_at FROM t_05396_run_marker)
    AND log_comment = '05396_root_none_insert';

EXECUTE AS user_05396_root_mv_caller SELECT count() = 0,
    countIf(notEmpty(databases) OR notEmpty(tables) OR notEmpty(columns) OR notEmpty(partitions)
        OR notEmpty(projections) OR notEmpty(views) OR notEmpty(used_row_policies)) = 0
FROM system.user_query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND initial_query_id = query_id
    AND initial_user = 'user_05396_root_mv_caller'
    AND query_start_time_microseconds >= (SELECT started_at FROM t_05396_run_marker)
    AND log_comment = '05396_root_none_insert';

SELECT (SELECT count() FROM t_05396_root_definer_sink), (SELECT count() FROM t_05396_root_none_sink);

DROP USER user_05396_mv_caller;
DROP USER user_05396_root_mv_caller;
DROP USER user_05396_invoker;
DROP TABLE mv_05396_nested;
DROP TABLE mv_05396_direct_definer;
DROP TABLE mv_05396_direct_none;
DROP TABLE mv_05396_multi_definer;
DROP TABLE mv_05396_multi_none;
DROP TABLE mv_05396_root_definer;
DROP TABLE mv_05396_root_none;
DROP VIEW v_05396_protected;
DROP VIEW v_05396_invoker;
DROP TABLE t_05396_invoker_expected_qai;
DROP TABLE t_05396_invoker_base;
DROP TABLE t_05396_sink;
DROP TABLE t_05396_direct_definer_sink;
DROP TABLE t_05396_direct_none_sink;
DROP TABLE t_05396_multi_none_sink;
DROP TABLE t_05396_multi_sink;
DROP TABLE t_05396_root_definer_sink;
DROP TABLE t_05396_root_none_sink;
DROP TABLE t_05396_root_protected;
DROP TABLE t_05396_direct_definer_source;
DROP TABLE t_05396_direct_none_source;
DROP TABLE t_05396_multi_source;
DROP TABLE t_05396_source;
DROP TABLE t_05396_protected;
DROP TABLE t_05396_run_marker;
