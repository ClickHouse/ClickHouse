DROP TABLE IF EXISTS t_optimize_equal_ranges;

CREATE TABLE t_optimize_equal_ranges (a UInt64, b String, c UInt64) ENGINE = MergeTree ORDER BY a;

SET max_block_size = 1024;
SET max_bytes_before_external_group_by = 0;
SET optimize_aggregation_in_order = 0;

INSERT INTO t_optimize_equal_ranges SELECT 0, toString(number), number FROM numbers(30000);
INSERT INTO t_optimize_equal_ranges SELECT 1, toString(number), number FROM numbers(30000);
INSERT INTO t_optimize_equal_ranges SELECT 2, toString(number), number FROM numbers(30000);

SELECT a, uniqExact(b) FROM t_optimize_equal_ranges GROUP BY a ORDER BY a;
SELECT a, sum(c) FROM t_optimize_equal_ranges GROUP BY a ORDER BY a;

SYSTEM FLUSH LOGS;

SELECT
    used_aggregate_functions[1] AS func,
    ProfileEvents['AggregationOptimizedEqualRangesOfKeys'] > 0
FROM system.query_log
WHERE type = 'QueryFinish' AND current_database = currentDatabase() AND query LIKE '%SELECT%FROM%t_optimize_equal_ranges%'
ORDER BY func;

DROP TABLE t_optimize_equal_ranges;
