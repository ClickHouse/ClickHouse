-- test that multiple refreshes of RMV with multiple subqueries do not leak memory or cause any other general issues with the instance 
CREATE TABLE 03789_rmv_target (message String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test/03789_rmv_with_multiple_subqueries', 'r1') ORDER BY ();
CREATE MATERIALIZED VIEW 03789_rmv_mv REFRESH EVERY 1 MONTH APPEND TO 03789_rmv_target AS WITH
    (
        SELECT 1
    ) AS lower_limit,
    (
        SELECT number
        FROM numbers(10)
        WHERE number = lower_limit
    ) AS upper_limit,
    result AS
    (
        SELECT 'OH NO' AS message
        FROM numbers(10)
        WHERE (number >= lower_limit) AND (number <= upper_limit)
    )
SELECT *
FROM result;

SYSTEM REFRESH VIEW 03789_rmv_mv;
SYSTEM WAIT VIEW 03789_rmv_mv;
SYSTEM REFRESH VIEW 03789_rmv_mv;
SYSTEM WAIT VIEW 03789_rmv_mv;

SYSTEM FLUSH LOGS query_log;
SELECT uniqExact(query) FROM system.query_log WHERE has(databases, currentDatabase()) AND query LIKE '%INSERT%SELECT%' AND type = 'QueryFinish';