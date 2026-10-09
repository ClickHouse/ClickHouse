-- a projection with WHERE stores only the rows that pass its filter, and a part with no such rows has no projection part
-- the twin table has the projection materialized, and the query log shows the marks that the optimizer selects

SET optimize_use_projections = 1, optimize_use_implicit_projections = 0, prefer_optimize_projection = 0, force_optimize_projection = 0, enable_parallel_replicas = 0, log_queries = 1;

DROP TABLE IF EXISTS t_est;
DROP TABLE IF EXISTS t_real;

CREATE TABLE t_est (a UInt64, b UInt64, v UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 100, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
        merge_max_block_size = 8192;
CREATE TABLE t_real AS t_est;
ALTER TABLE t_real ADD PROJECTION p (SELECT a, b, v WHERE v % 10 = 0 ORDER BY b);

-- the second part has no row that passes the filter
INSERT INTO t_est SELECT number, number % 100, number FROM numbers(30000);
INSERT INTO t_est SELECT number + 30000, number % 100, number * 10 + 1 FROM numbers(5000);
INSERT INTO t_real SELECT number, number % 100, number FROM numbers(30000);
INSERT INTO t_real SELECT number + 30000, number % 100, number * 10 + 1 FROM numbers(5000);

CREATE HYPOTHETICAL PROJECTION p ON t_est (SELECT a, b, v WHERE v % 10 = 0 ORDER BY b);

SELECT '-- the query filter implies the filter of the projection';
SELECT trim(explain) FROM (EXPLAIN WHATIF projection_scan_budget_rows = 0 SELECT a, b, v FROM t_est WHERE b = 40 AND v % 10 = 0)
WHERE match(trim(explain), '^(marks|status|verdict):');
SELECT count() FROM t_real WHERE b = 40 AND v % 10 = 0 SETTINGS log_comment = '05346_implies' FORMAT Null;

SELECT '-- the same with a sample, the span contains the read of the optimizer';
SELECT trim(explain) FROM (EXPLAIN WHATIF projection_scan_budget_rows = 3000 SELECT a, b, v FROM t_est WHERE b = 40 AND v % 10 = 0)
WHERE match(trim(explain), '^(marks_span|verdict):');

SELECT '-- the query filter does not imply it';
SELECT trim(explain) FROM (EXPLAIN WHATIF SELECT a, b, v FROM t_est WHERE b = 40)
WHERE match(trim(explain), '^(status|reason):');
SELECT count() FROM t_real WHERE b = 40 SETTINGS log_comment = '05346_not_implies' FORMAT Null;

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['SelectedMarks'], length(projections) FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment IN ('05346_implies', '05346_not_implies') AND type = 'QueryFinish'
ORDER BY log_comment;

DROP TABLE t_est;
DROP TABLE t_real;
