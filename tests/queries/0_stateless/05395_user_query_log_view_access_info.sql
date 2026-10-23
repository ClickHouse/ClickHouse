-- Tags: no-parallel
-- Tag no-parallel: copies share global users with fixed names.

-- Query-log access metadata remains complete for operators. Own-log users only see initial
-- rows with an explicit unhidden marker; marked DEFINER/NONE QueryFinish rows are omitted.
SET log_queries = 1;
SET log_queries_probability = 1;
SET log_queries_min_type = 'QUERY_START';
SET analyzer_inline_views = 0;
SET optimize_use_projections = 1;
SET optimize_aggregation_in_order = 0;
SET optimize_read_in_order = 0;
SET use_query_cache = 0;

DROP USER IF EXISTS user_05395_sealed;
DROP USER IF EXISTS user_05395_invoker;
DROP VIEW IF EXISTS v_05395_definer;
DROP VIEW IF EXISTS v_05395_none;
DROP VIEW IF EXISTS v_05395_invoker;
DROP TABLE IF EXISTS t_05395_invoker_expected_qai;
DROP TABLE IF EXISTS t_05395_direct_expected_qai;
DROP TABLE IF EXISTS t_05395_remote_query_id;
DROP TABLE IF EXISTS t_05395_private;

CREATE TABLE t_05395_private
(
    id UInt64,
    name String,
    PROJECTION pn (SELECT id, name ORDER BY name, id)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t_05395_private VALUES (1, 'alpha'), (2, 'beta'), (3, 'alpha');

CREATE VIEW v_05395_definer DEFINER = CURRENT_USER SQL SECURITY DEFINER AS
SELECT id, name FROM t_05395_private WHERE id > 0;

CREATE VIEW v_05395_none DEFINER = CURRENT_USER SQL SECURITY NONE AS
SELECT id, name FROM t_05395_private WHERE id > 0;

CREATE VIEW v_05395_invoker SQL SECURITY INVOKER AS
SELECT id, name FROM t_05395_private;

CREATE USER user_05395_sealed;
GRANT SELECT ON v_05395_definer TO user_05395_sealed;
GRANT SELECT ON v_05395_none TO user_05395_sealed;
GRANT REMOTE ON *.* TO user_05395_sealed;
GRANT CREATE TEMPORARY TABLE ON *.* TO user_05395_sealed;

CREATE USER user_05395_invoker;
GRANT SELECT ON v_05395_invoker TO user_05395_invoker;
GRANT SELECT ON t_05395_private TO user_05395_invoker;

-- Expose only the authorized control rows and their arrays, never operator query_log access.
CREATE TABLE t_05395_invoker_expected_qai
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
GRANT SELECT ON t_05395_invoker_expected_qai TO user_05395_invoker;

-- Keep the direct-query snapshot separate; its arrays are visible only to the current operator.
CREATE TABLE t_05395_direct_expected_qai
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

-- This run marker gives the impersonated reader a fresh lower bound without granting query-log access.
CREATE TABLE t_05395_run_marker (started_at DateTime64(6, 'UTC')) ENGINE = Memory;
INSERT INTO t_05395_run_marker SELECT now64(6, 'UTC');
GRANT SELECT ON t_05395_run_marker TO user_05395_sealed;
GRANT SELECT ON t_05395_run_marker TO user_05395_invoker;

-- The sealed reader may inspect only this test-owned parent query ID.
CREATE TABLE t_05395_remote_query_id (query_id String) ENGINE = Memory;
GRANT SELECT ON t_05395_remote_query_id TO user_05395_sealed;

-- This reader has only view grants; direct access to the private table remains denied.
EXECUTE AS user_05395_sealed SELECT count() FROM t_05395_private; -- { serverError ACCESS_DENIED }

EXECUTE AS user_05395_sealed SELECT name FROM v_05395_definer GROUP BY name ORDER BY name
FORMAT Null SETTINGS force_optimize_projection = 1, force_optimize_projection_name = 'pn', log_comment = '05395_definer';

EXECUTE AS user_05395_sealed SELECT name FROM v_05395_none GROUP BY name ORDER BY name
FORMAT Null SETTINGS force_optimize_projection = 1, force_optimize_projection_name = 'pn', log_comment = '05395_none';

EXECUTE AS user_05395_invoker SELECT name FROM v_05395_invoker GROUP BY name ORDER BY name
FORMAT Null SETTINGS force_optimize_projection = 1, force_optimize_projection_name = 'pn', log_comment = '05395_invoker';

-- This is a true top-level query as the test runner's current user, not an EXECUTE AS wrapper.
SELECT name FROM t_05395_private WHERE id > 0 GROUP BY name ORDER BY name
FORMAT Null SETTINGS force_optimize_projection = 1, force_optimize_projection_name = 'pn', log_comment = '05395_direct';

-- Keep the projection selector out of the caller's SQL text while forcing the remote view query to use it.
SET force_optimize_projection = 1;
SET force_optimize_projection_name = 'pn';
EXECUTE AS user_05395_sealed SELECT name
FROM remote('127.0.0.1', currentDatabase(), 'v_05395_definer', 'user_05395_sealed')
GROUP BY name ORDER BY name
FORMAT Null SETTINGS prefer_localhost_replica = 0, log_comment = '05395_remote_definer';
SET force_optimize_projection = 0;
SET force_optimize_projection_name = '';

SYSTEM FLUSH LOGS query_log;

-- Save the exact outer query ID; children may have different IDs and comments.
INSERT INTO t_05395_remote_query_id
SELECT query_id
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query_id = initial_query_id
    AND initial_user = 'user_05395_sealed'
    AND query_start_time_microseconds >= (SELECT started_at FROM t_05395_run_marker)
    AND log_comment = '05395_remote_definer';

-- Snapshot only the INVOKER-view operator row for exact per-query comparison by that invoker.
INSERT INTO t_05395_invoker_expected_qai
SELECT query_id, databases, tables, columns, partitions, projections, views, used_row_policies
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND initial_query_id = query_id
    AND initial_user = 'user_05395_invoker'
    AND query_start_time_microseconds >= (SELECT started_at FROM t_05395_run_marker)
    AND log_comment = '05395_invoker';

-- Snapshot the true top-level direct query for a separate exact own-log comparison.
INSERT INTO t_05395_direct_expected_qai
SELECT query_id, databases, tables, columns, partitions, projections, views, used_row_policies
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND initial_query_id = query_id
    AND initial_user = currentUser()
    AND query_start_time_microseconds >= (SELECT started_at FROM t_05395_run_marker)
    AND log_comment = '05395_direct';

-- Operator audit keeps the inner table and projection attribution for both security modes.
SELECT
    countIf(log_comment = '05395_definer' AND query_access_info_is_hidden = 1
        AND initial_user = 'user_05395_sealed'
        AND has(tables, concat(currentDatabase(), '.t_05395_private'))
        AND has(projections, concat(currentDatabase(), '.t_05395_private.pn'))) = 1,
    countIf(log_comment = '05395_none' AND query_access_info_is_hidden = 1
        AND initial_user = 'user_05395_sealed'
        AND has(tables, concat(currentDatabase(), '.t_05395_private'))
        AND has(projections, concat(currentDatabase(), '.t_05395_private.pn'))) = 1,
    countIf(log_comment = '05395_invoker' AND query_access_info_is_hidden = 0
        AND initial_user = 'user_05395_invoker'
        AND has(tables, concat(currentDatabase(), '.t_05395_private'))
        AND has(projections, concat(currentDatabase(), '.t_05395_private.pn'))) = 1,
    countIf(log_comment = '05395_direct' AND query_access_info_is_hidden = 0
        AND initial_user = currentUser()
        AND has(tables, concat(currentDatabase(), '.t_05395_private'))
        AND has(projections, concat(currentDatabase(), '.t_05395_private.pn'))) = 1
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND initial_query_id = query_id
    AND query_start_time_microseconds >= (SELECT started_at FROM t_05395_run_marker)
    AND log_comment IN ('05395_definer', '05395_none', '05395_invoker', '05395_direct');

-- Require a real loopback child with both events and hidden private table/projection attribution.
-- The remote child can record a different current_database; correlate it by initial_query_id.
SELECT
    (SELECT count() = 1 FROM t_05395_remote_query_id) AND count() > 0
FROM
(
    SELECT query_id
    FROM system.query_log
    WHERE initial_query_id = (SELECT any(query_id) FROM t_05395_remote_query_id)
        AND query_id != (SELECT any(query_id) FROM t_05395_remote_query_id)
    GROUP BY query_id
    HAVING countIf(type = 'QueryStart') > 0
        AND countIf(type = 'QueryFinish'
            AND query_access_info_is_hidden = 1
            AND has(tables, concat(currentDatabase(), '.t_05395_private'))
            AND has(projections, concat(currentDatabase(), '.t_05395_private.pn'))) > 0
);

-- The caller can retain its initial QueryStart, but sees no child rows or private tokens in any event.
EXECUTE AS user_05395_sealed SELECT
    countIf(query_id = (SELECT any(query_id) FROM t_05395_remote_query_id) AND type = 'QueryStart') <= 1,
    countIf(query_id != (SELECT any(query_id) FROM t_05395_remote_query_id)) = 0,
    countIf(
        arrayExists(token -> position(token, 't_05395_private') > 0,
            arrayConcat(databases, tables, columns, partitions, projections, views, used_row_policies))
        OR has(projections, concat(currentDatabase(), '.t_05395_private.pn'))
        OR position(query, 't_05395_private') > 0
        OR match(query, '(^|[^a-zA-Z0-9_])pn([^a-zA-Z0-9_]|$)')
        OR position(formatted_query, 't_05395_private') > 0
        OR match(formatted_query, '(^|[^a-zA-Z0-9_])pn([^a-zA-Z0-9_]|$)')
        OR position(exception, 't_05395_private') > 0
        OR match(exception, '(^|[^a-zA-Z0-9_])pn([^a-zA-Z0-9_]|$)')) = 0
FROM system.user_query_log
WHERE initial_query_id = (SELECT any(query_id) FROM t_05395_remote_query_id);

-- The sealed-view reader cannot see the marked initial QueryFinish rows at all.
EXECUTE AS user_05395_sealed SELECT
    count() = 0,
    countIf(notEmpty(databases) OR notEmpty(tables) OR notEmpty(columns) OR notEmpty(partitions)
        OR notEmpty(projections) OR notEmpty(views) OR notEmpty(used_row_policies)) = 0,
    countIf(initial_user = 'user_05395_sealed') = 0
FROM system.user_query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND initial_query_id = query_id
    AND initial_user = 'user_05395_sealed'
    AND query_start_time_microseconds >= (SELECT started_at FROM t_05395_run_marker)
    AND log_comment IN ('05395_definer', '05395_none');

-- An outer expression cannot use a hidden name as an error oracle.
EXECUTE AS user_05395_sealed SELECT count()
FROM system.user_query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND initial_query_id = query_id
    AND initial_user = 'user_05395_sealed'
    AND query_start_time_microseconds >= (SELECT started_at FROM t_05395_run_marker)
    AND log_comment IN ('05395_definer', '05395_none')
    AND throwIf(arrayExists(name -> name = concat(currentDatabase(), '.t_05395_private'), tables)
        OR arrayExists(name -> name = concat(currentDatabase(), '.t_05395_private.pn'), projections),
        'private view metadata leaked') = 0;

-- ARRAY JOIN cannot expose a hidden projection name to the outer query either.
EXECUTE AS user_05395_sealed SELECT count()
FROM system.user_query_log
ARRAY JOIN projections AS projection_name
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND initial_query_id = query_id
    AND initial_user = 'user_05395_sealed'
    AND query_start_time_microseconds >= (SELECT started_at FROM t_05395_run_marker)
    AND log_comment IN ('05395_definer', '05395_none')
    AND throwIf(projection_name = concat(currentDatabase(), '.t_05395_private.pn'),
        'private projection name leaked') = 0;

-- The unmarked INVOKER arrays exactly match its one operator query ID.
-- The invoker can read only this run's qid+array snapshot, not operator query_log.
EXECUTE AS user_05395_invoker SELECT
    count() = 1,
    uniqExact(caller.query_id) = 1,
    countIf(caller.databases = expected.databases
        AND caller.tables = expected.tables
        AND caller.columns = expected.columns
        AND caller.partitions = expected.partitions
        AND caller.projections = expected.projections
        AND caller.views = expected.views
        AND caller.used_row_policies = expected.used_row_policies) = 1,
    (SELECT count() = 1 AND uniqExact(query_id) = 1 FROM t_05395_invoker_expected_qai)
FROM system.user_query_log AS caller
INNER JOIN t_05395_invoker_expected_qai AS expected ON caller.query_id = expected.query_id
WHERE caller.current_database = currentDatabase()
    AND caller.type = 'QueryFinish'
    AND caller.initial_query_id = caller.query_id
    AND caller.initial_user = 'user_05395_invoker'
    AND caller.query_start_time_microseconds >= (SELECT started_at FROM t_05395_run_marker)
    AND caller.log_comment = '05395_invoker';

-- The current user sees the same seven arrays for its true top-level direct query.
SELECT
    count() = 1,
    uniqExact(caller.query_id) = 1,
    countIf(caller.databases = expected.databases
        AND caller.tables = expected.tables
        AND caller.columns = expected.columns
        AND caller.partitions = expected.partitions
        AND caller.projections = expected.projections
        AND caller.views = expected.views
        AND caller.used_row_policies = expected.used_row_policies) = 1,
    (SELECT count() = 1 AND uniqExact(query_id) = 1 FROM t_05395_direct_expected_qai)
FROM system.user_query_log AS caller
INNER JOIN t_05395_direct_expected_qai AS expected ON caller.query_id = expected.query_id
WHERE caller.current_database = currentDatabase()
    AND caller.type = 'QueryFinish'
    AND caller.initial_query_id = caller.query_id
    AND caller.initial_user = currentUser()
    AND caller.query_start_time_microseconds >= (SELECT started_at FROM t_05395_run_marker)
    AND caller.log_comment = '05395_direct';

DROP USER user_05395_invoker;
DROP USER user_05395_sealed;
DROP VIEW v_05395_invoker;
DROP VIEW v_05395_none;
DROP VIEW v_05395_definer;
DROP TABLE t_05395_invoker_expected_qai;
DROP TABLE t_05395_direct_expected_qai;
DROP TABLE t_05395_remote_query_id;
DROP TABLE t_05395_private;
DROP TABLE t_05395_run_marker;
