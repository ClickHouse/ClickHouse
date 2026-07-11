-- `max_rows_to_group_by` bounds the number of GROUP BY keys held in the aggregation state at once. Ordinary
-- aggregation-in-order keeps only a small working set of keys (it streams out completed groups as the sorted
-- input advances), so this limit is effectively never reached and does not affect the result - unlike the
-- default hash aggregation, which holds all keys at once and throws. The shuffle path bypasses the funnel and
-- must not change this: it is disabled when `max_rows_to_group_by` is set, so such a query keeps behaving
-- exactly like ordinary aggregation-in-order.

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
               max_rows_to_group_by = 10, group_by_overflow_mode = 'throw');

-- With `aggregation_in_order_shuffle` enabled and the limit set, the result must be identical to ordinary
-- aggregation-in-order with the same limit (the shuffle path is transparently disabled, and the limit does
-- not apply to in-order aggregation, so both keep all 5000 groups).
SELECT
    (SELECT groupBitXor(cityHash64(k, s)) FROM (SELECT k, sum(v) s FROM t_aio_shuffle_limits GROUP BY k)
        SETTINGS optimize_aggregation_in_order = 1, aggregation_in_order_shuffle = 1, max_threads = 4,
                 max_rows_to_group_by = 10, group_by_overflow_mode = 'throw')
  = (SELECT groupBitXor(cityHash64(k, s)) FROM (SELECT k, sum(v) s FROM t_aio_shuffle_limits GROUP BY k)
        SETTINGS optimize_aggregation_in_order = 1, aggregation_in_order_shuffle = 0, max_threads = 4,
                 max_rows_to_group_by = 10, group_by_overflow_mode = 'throw');

DROP TABLE t_aio_shuffle_limits;
