-- a projection with WHERE stores only the rows that pass its filter, and a part with no such rows has no projection part
-- the twin table has the projection materialized and shows the read that the optimizer selects

SET optimize_use_projections = 1, optimize_use_implicit_projections = 0, prefer_optimize_projection = 0, force_optimize_projection = 0, enable_parallel_replicas = 0, log_queries = 1;

DROP TABLE IF EXISTS t_est;
DROP TABLE IF EXISTS t_real;

CREATE TABLE t_est (a UInt64, b UInt64, v UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 100, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
        merge_max_block_size = 8192;
CREATE TABLE t_real AS t_est;
ALTER TABLE t_real ADD PROJECTION p (SELECT a, b, v WHERE v % 10 = 0 ORDER BY b);

-- the cases need the two parts, the second part has no row that passes the filter
SYSTEM STOP MERGES t_est;
SYSTEM STOP MERGES t_real;
INSERT INTO t_est SELECT number, number % 100, number FROM numbers(30000);
INSERT INTO t_est SELECT number + 30000, number % 100, number * 10 + 1 FROM numbers(5000);
INSERT INTO t_real SELECT number, number % 100, number FROM numbers(30000);
INSERT INTO t_real SELECT number + 30000, number % 100, number * 10 + 1 FROM numbers(5000);

CREATE HYPOTHETICAL PROJECTION p ON t_est (SELECT a, b, v WHERE v % 10 = 0 ORDER BY b);

SELECT '-- the query filter implies the filter of the projection, the optimizer reads the second part from the base table';
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF projection_scan_budget_rows = 0 SELECT a, b, v FROM t_est WHERE b = 40 AND v % 10 = 0)
WHERE match(line, '^(Baseline|With |marks:|rows:|status:|verdict:)');
SELECT 'real: ' || toString(marks) || ' marks, ' || toString(rows) || ' rows'
FROM (EXPLAIN ESTIMATE SELECT a, b, v FROM t_real WHERE b = 40 AND v % 10 = 0);
SELECT count() FROM t_real WHERE b = 40 AND v % 10 = 0 SETTINGS log_comment = '05346_implies' FORMAT Null;

SELECT '-- a sample of the first part, the span contains the read of the optimizer';
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF projection_scan_budget_rows = 3000 SELECT a, b, v FROM t_est WHERE a < 30000 AND b = 40 AND v % 10 = 0)
WHERE match(line, '^(Baseline|With |marks:|marks_span|verdict|sampled_marks)');
SELECT 'real: ' || toString(marks) || ' marks, ' || toString(rows) || ' rows'
FROM (EXPLAIN ESTIMATE SELECT a, b, v FROM t_real WHERE a < 30000 AND b = 40 AND v % 10 = 0);

SELECT '-- the query filter does not imply it';
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF SELECT a, b, v FROM t_est WHERE b = 40)
WHERE match(line, '^(status|reason):');
SELECT count() FROM t_real WHERE b = 40 SETTINGS log_comment = '05346_not_implies' FORMAT Null;

SYSTEM FLUSH LOGS query_log;
SELECT 'real, ' || if(log_comment = '05346_implies', 'filter implied', 'filter not implied') || ': '
    || toString(ProfileEvents['SelectedMarks']) || ' marks, ' || if(empty(projections), 'from the base table', 'from the projection')
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment IN ('05346_implies', '05346_not_implies') AND type = 'QueryFinish'
ORDER BY log_comment;

-- the rows that pass the filter are in one granule, a sample that misses it shows no projection part
CREATE TABLE c_est (a UInt64, b UInt64, v UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 100, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO c_est SELECT number, number % 100, intDiv(number, 100) FROM numbers(10000);
CREATE HYPOTHETICAL PROJECTION p ON c_est (SELECT a, b, v WHERE v = 0 ORDER BY b);
SELECT '-- the rows of the filter in one granule, with a full scan';
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF projection_scan_budget_rows = 0 SELECT a, b, v FROM c_est WHERE b = 40 AND v = 0)
WHERE match(line, '^(Baseline|With |status:|marks:|verdict:)');
SELECT '-- the same with a sample';
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF projection_scan_budget_rows = 3000 SELECT a, b, v FROM c_est WHERE b = 40 AND v = 0)
WHERE match(line, '^(empirical_status|empirical_reason):');

-- a projection part keeps only the rows that exist after a lightweight delete
CREATE TABLE d_est (a UInt64, b UInt64, v UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 100, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
CREATE TABLE d_real AS d_est;
INSERT INTO d_est SELECT number, number % 100, number FROM numbers(10000);
INSERT INTO d_real SELECT number, number % 100, number FROM numbers(10000);
DELETE FROM d_est WHERE a < 9000;
DELETE FROM d_real WHERE a < 9000;
ALTER TABLE d_real ADD PROJECTION p (SELECT a, b, v WHERE v % 10 = 0 ORDER BY b);
ALTER TABLE d_real MATERIALIZE PROJECTION p SETTINGS mutations_sync = 2;
CREATE HYPOTHETICAL PROJECTION p ON d_est (SELECT a, b, v WHERE v % 10 = 0 ORDER BY b);
SELECT '-- a part with a lightweight delete';
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF projection_scan_budget_rows = 0 SELECT a, b, v FROM d_est WHERE b = 40 AND v % 10 = 0)
WHERE match(line, '^(With |marks:|rows:|verdict:)');
SELECT 'real: ' || toString(marks) || ' marks, ' || toString(rows) || ' rows'
FROM (EXPLAIN ESTIMATE SELECT a, b, v FROM d_real WHERE b = 40 AND v % 10 = 0);

DROP TABLE t_est;
DROP TABLE t_real;
DROP TABLE c_est;
DROP TABLE d_est;
DROP TABLE d_real;
