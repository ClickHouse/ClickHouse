-- Setup
DROP TABLE IF EXISTS t_row_policy;
DROP TABLE IF EXISTS t_row_policy_dest;
DROP VIEW IF EXISTS v_row_policy;
DROP ROW POLICY IF EXISTS p_filter ON t_row_policy;
DROP ROW POLICY IF EXISTS p_filter2 ON t_row_policy;

CREATE TABLE t_row_policy (x UInt8) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_row_policy VALUES (1), (2), (3), (4);
CREATE TABLE t_row_policy_dest (x UInt8) ENGINE = MergeTree ORDER BY x;
CREATE VIEW v_row_policy AS SELECT * FROM t_row_policy;
CREATE ROW POLICY p_filter ON t_row_policy USING x >= 2 AS permissive TO ALL;
CREATE ROW POLICY p_filter2 ON t_row_policy USING x < 4 AS restrictive TO ALL;

-- Test: View
SELECT 'View';
SELECT * FROM v_row_policy ORDER BY x;

-- Test: Nested subquery
SELECT 'Nested subquery';
SELECT * FROM (SELECT * FROM (SELECT * FROM t_row_policy)) ORDER BY x;

-- Test: INSERT SELECT
SELECT 'INSERT SELECT';
INSERT INTO t_row_policy_dest SELECT * FROM t_row_policy;
SELECT * FROM t_row_policy_dest ORDER BY x;

-- Check query_log
SYSTEM FLUSH LOGS query_log;

SELECT 'query_log';
SELECT
    query,
    used_row_policies
FROM system.query_log
WHERE
    event_date >= yesterday()
    AND event_time >= now() - 600
    AND current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND (
        query LIKE '%SELECT%FROM v_row_policy%'
        OR query LIKE '%SELECT%FROM (SELECT%FROM (SELECT%FROM t_row_policy%'
        OR query LIKE '%INSERT INTO t_row_policy_dest SELECT%'
    )
    AND query NOT LIKE '%system.query_log%'
ORDER BY event_time_microseconds;

-- Cleanup
DROP VIEW IF EXISTS v_row_policy;
DROP ROW POLICY IF EXISTS p_filter ON t_row_policy;
DROP ROW POLICY IF EXISTS p_filter2 ON t_row_policy;
DROP TABLE IF EXISTS t_row_policy;
DROP TABLE IF EXISTS t_row_policy_dest;
