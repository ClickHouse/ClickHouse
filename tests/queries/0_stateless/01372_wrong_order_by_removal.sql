CREATE TEMPORARY TABLE moving_sum_num
(
    `k` String,
    `dt` DateTime,
    `v` UInt64
);

SET enable_debug_queries = 1;

-- ORDER BY from subquery shall not be removed.
ANALYZE SELECT k, groupArrayMovingSum(v) FROM (SELECT * FROM moving_sum_num ORDER BY k, dt) GROUP BY k ORDER BY k;
