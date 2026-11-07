-- Keep this test scoped to query IDs created by this native-client session so a
-- previous invocation cannot satisfy the query-log assertions.
SET enable_parallel_replicas = 0;
SET log_queries = 1;
SET log_queries_probability = 1;
SET log_queries_min_type = 'QUERY_START';
SET session_query_ids_history_size = 100;
SET optimize_use_projections = 1;
SET optimize_aggregation_in_order = 0;
SET optimize_read_in_order = 0;
SET use_query_cache = 0;

CREATE TABLE t_05394_projection_view
(
    id UInt64,
    name String,
    PROJECTION projection_name
    (
        SELECT sum(id) GROUP BY id, name
    ),
    PROJECTION pn
    (
        SELECT id, name ORDER BY name, id
    )
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t_05394_projection_view VALUES (1, 'alpha'), (2, 'beta'), (3, 'alpha');

CREATE VIEW v_05394_projection_inline DEFINER = CURRENT_USER SQL SECURITY DEFINER AS
SELECT id, name FROM t_05394_projection_view;

CREATE VIEW v_05394_projection_sealed DEFINER = CURRENT_USER SQL SECURITY DEFINER AS
SELECT id, name FROM t_05394_projection_view WHERE id > 0;

SELECT name FROM t_05394_projection_view GROUP BY name ORDER BY name
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1,
         force_optimize_projection_name = 'projection_name', optimize_read_in_order = 0,
         use_query_cache = 0, log_comment = '05394_direct';

SELECT name FROM v_05394_projection_inline GROUP BY name ORDER BY name
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1,
         force_optimize_projection_name = 'projection_name', optimize_read_in_order = 0,
         use_query_cache = 0, log_comment = '05394_inline';

-- Check projection attribution with analyzer view inlining enabled.
SELECT name FROM v_05394_projection_inline GROUP BY name ORDER BY name
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1,
         force_optimize_projection_name = 'projection_name', analyzer_inline_views = 1,
         optimize_read_in_order = 0, use_query_cache = 0, log_comment = '05394_inline1';

SELECT name FROM v_05394_projection_sealed GROUP BY name ORDER BY name
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1,
         force_optimize_projection_name = 'pn', optimize_read_in_order = 0,
         use_query_cache = 0, log_comment = '05394_sealed';

SELECT name FROM t_05394_projection_view GROUP BY name
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1,
         force_optimize_projection_name = 'missing_05394_direct', optimize_read_in_order = 0,
         use_query_cache = 0, log_comment = '05394_missing_direct'; -- { serverError INCORRECT_DATA }

SELECT name FROM v_05394_projection_inline GROUP BY name ORDER BY name
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1,
         force_optimize_projection_name = 'missing_05394_inline1', analyzer_inline_views = 1,
         optimize_read_in_order = 0, use_query_cache = 0, log_comment = '05394_missing_inline1'; -- { serverError INCORRECT_DATA }

SELECT name FROM v_05394_projection_sealed GROUP BY name ORDER BY name
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1,
         force_optimize_projection_name = 'pn_missing_8826', optimize_read_in_order = 0,
         use_query_cache = 0, log_comment = '05394_missing_sealed'; -- { serverError INCORRECT_DATA }

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    countIf(type IN ('QueryFinish', 'ExceptionBeforeStart', 'ExceptionWhileProcessing')) AS terminal_count,
    countIf(type = 'QueryFinish' AND has(projections, concat(currentDatabase(), '.t_05394_projection_view.',
        if(log_comment = '05394_sealed', 'pn', 'projection_name')))) AS projection_count,
    countIf(type IN ('ExceptionBeforeStart', 'ExceptionWhileProcessing') AND exception_code = 117) AS expected_missing_projection_count
FROM system.query_log
WHERE current_database = currentDatabase()
    AND initial_query_id = query_id
    AND query_id IN (SELECT query_id FROM system.session_query_ids)
    AND type IN ('QueryFinish', 'ExceptionBeforeStart', 'ExceptionWhileProcessing')
    AND log_comment IN ('05394_direct', '05394_inline', '05394_inline1', '05394_sealed', '05394_missing_direct', '05394_missing_inline1', '05394_missing_sealed')
GROUP BY log_comment
ORDER BY log_comment;

-- Both exact top-level protected QueryStart and QueryFinish are hidden; the direct-query control remains visible.
SELECT
    countIf(log_comment = '05394_inline1' AND type = 'QueryStart'),
    countIf(log_comment = '05394_inline1' AND type = 'QueryStart' AND query_access_info_is_hidden = 1),
    countIf(log_comment = '05394_inline1' AND type = 'QueryFinish'),
    countIf(log_comment = '05394_inline1' AND type = 'QueryFinish' AND query_access_info_is_hidden = 1),
    countIf(log_comment = '05394_direct' AND type = 'QueryStart'),
    countIf(log_comment = '05394_direct' AND type = 'QueryStart' AND query_access_info_is_hidden = 0),
    countIf(log_comment = '05394_direct' AND type = 'QueryFinish'),
    countIf(log_comment = '05394_direct' AND type = 'QueryFinish' AND query_access_info_is_hidden = 0)
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type IN ('QueryStart', 'QueryFinish')
    AND initial_query_id = query_id
    AND query_id IN (SELECT query_id FROM system.session_query_ids)
    AND log_comment IN ('05394_inline1', '05394_direct');

SELECT
    countIf(log_comment = '05394_inline1' AND type = 'QueryStart'),
    countIf(log_comment = '05394_inline1' AND type = 'QueryFinish'),
    countIf(log_comment = '05394_direct' AND type = 'QueryStart'),
    countIf(log_comment = '05394_direct' AND type = 'QueryFinish')
FROM system.user_query_log
WHERE current_database = currentDatabase()
    AND type IN ('QueryStart', 'QueryFinish')
    AND initial_query_id = query_id
    AND query_id IN (SELECT query_id FROM system.session_query_ids)
    AND log_comment IN ('05394_inline1', '05394_direct');

DROP VIEW v_05394_projection_sealed;
DROP VIEW v_05394_projection_inline;
DROP TABLE t_05394_projection_view;
