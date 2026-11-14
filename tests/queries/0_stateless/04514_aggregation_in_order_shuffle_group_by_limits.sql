-- The shuffle path bypasses the single-threaded funnel that ordinary aggregation-in-order uses, so to keep
-- the behavior of a query that sets `max_rows_to_group_by` unchanged, the shuffle is disabled whenever that
-- limit is set (any non-zero value). This test asserts (a) the shuffle is indeed not used when the limit is
-- set, and (b) the result is identical to ordinary aggregation-in-order.
--
-- The limit is set high enough that it is never reached (there are only 5000 distinct keys), so the query
-- completes without throwing whichever aggregation method the plan picks - it may be ordinary
-- aggregation-in-order (a bounded working set) or, e.g. under the old analyzer for the nested subquery, plain
-- hash aggregation (all keys at once). Either way both `aggregation_in_order_shuffle` = 1 (transparently
-- disabled by the gate) and = 0 run the same plan and return all 5000 groups.

SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_aio_shuffle_limits;

CREATE TABLE t_aio_shuffle_limits (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 128;

SYSTEM STOP MERGES t_aio_shuffle_limits;
INSERT INTO t_aio_shuffle_limits SELECT number % 5000 AS k, number FROM numbers(50000);
INSERT INTO t_aio_shuffle_limits SELECT number % 5000 AS k, number FROM numbers(50000);

-- The shuffle must not be used when `max_rows_to_group_by` is set.
SELECT countIf(explain LIKE '%BufferedShardByHashTransform%') = 0
FROM (EXPLAIN PIPELINE SELECT k, sum(v) FROM t_aio_shuffle_limits GROUP BY k
      SETTINGS max_threads = 4, optimize_aggregation_in_order = 1, aggregation_in_order_shuffle = 1,
               max_rows_to_group_by = 100000, group_by_overflow_mode = 'throw');

-- With `aggregation_in_order_shuffle` enabled and the limit set, the result must be identical to ordinary
-- aggregation-in-order with the same limit (the shuffle path is transparently disabled, and the limit is
-- never reached, so both keep all 5000 groups).
SELECT
    (SELECT groupBitXor(cityHash64(k, s)) FROM (SELECT k, sum(v) s FROM t_aio_shuffle_limits GROUP BY k)
        SETTINGS optimize_aggregation_in_order = 1, aggregation_in_order_shuffle = 1, max_threads = 4,
                 max_rows_to_group_by = 100000, group_by_overflow_mode = 'throw')
  = (SELECT groupBitXor(cityHash64(k, s)) FROM (SELECT k, sum(v) s FROM t_aio_shuffle_limits GROUP BY k)
        SETTINGS optimize_aggregation_in_order = 1, aggregation_in_order_shuffle = 0, max_threads = 4,
                 max_rows_to_group_by = 100000, group_by_overflow_mode = 'throw');

DROP TABLE t_aio_shuffle_limits;
