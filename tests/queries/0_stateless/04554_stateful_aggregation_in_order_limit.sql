-- Regression test for PR https://github.com/ClickHouse/ClickHouse/pull/110188
-- An outer `LIMIT` above a stateful projection (here `neighbor`) must not be pushed
-- into in-order aggregation early termination by `optimizeLimitForAggregationInOrder`.
-- Otherwise `AggregatingInOrderTransform` stops after `limit_hint` groups, the stateful
-- function sees a truncated stream, and `neighbor(key, 1)` returns the default `0`
-- instead of the next key.

DROP TABLE IF EXISTS t_04554;

CREATE TABLE t_04554 (key UInt64, val UInt64) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_04554 SELECT number % 5 AS key, number AS val FROM numbers(100);

-- Groups in key order are 0, 1, 2, 3, 4; the top group is key = 0 and neighbor(0, 1) = 1.
SELECT neighbor(key, 1)
FROM t_04554
GROUP BY key
ORDER BY key
LIMIT 1
SETTINGS allow_deprecated_error_prone_window_functions = 1,
         optimize_aggregation_in_order = 1,
         optimize_aggregation_in_order_limit = 1,
         max_threads = 1,
         max_block_size = 65536,
         enable_analyzer = 1;

SELECT neighbor(key, 1)
FROM t_04554
GROUP BY key
ORDER BY key
LIMIT 1
SETTINGS allow_deprecated_error_prone_window_functions = 1,
         optimize_aggregation_in_order = 1,
         optimize_aggregation_in_order_limit = 1,
         max_threads = 1,
         max_block_size = 65536,
         enable_analyzer = 0;

DROP TABLE t_04554;
