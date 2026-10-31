-- The repartitioning stage of `aggregation_in_order_shuffle` must not buffer without limit. Parts that each
-- contain a single GROUP BY key are the worst case: a per-shard sorted merge can finish only after the
-- scatters of the *other* parts reach EOF, so a scatter has to read its whole part into one shard's queue
-- while that shard's merge is blocked. With long single-key runs (many chunks per part) this buffering is
-- deterministically large, so a tiny `aggregation_in_order_shuffle_max_buffered_bytes` must fail the query
-- instead of buffering everything.

SET enable_parallel_replicas = 0;
SET read_in_order_use_virtual_row = 0;

-- The shuffle is disabled when `max_rows_to_group_by` is set (see 04514). The stateless-test profile sets a
-- huge `max_rows_to_group_by` by default, which would disable the shuffle (and its buffer budget) for the
-- whole test, so reset it to 0.
SET max_rows_to_group_by = 0;

DROP TABLE IF EXISTS t_aio_shuffle_budget;

CREATE TABLE t_aio_shuffle_budget (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;

SYSTEM STOP MERGES t_aio_shuffle_budget;
INSERT INTO t_aio_shuffle_budget SELECT 1, number FROM numbers(500000);
INSERT INTO t_aio_shuffle_budget SELECT 2, number FROM numbers(500000);
INSERT INTO t_aio_shuffle_budget SELECT 3, number FROM numbers(500000);
INSERT INTO t_aio_shuffle_budget SELECT 4, number FROM numbers(500000);
INSERT INTO t_aio_shuffle_budget SELECT 5, number FROM numbers(500000);
INSERT INTO t_aio_shuffle_budget SELECT 6, number FROM numbers(500000);
INSERT INTO t_aio_shuffle_budget SELECT 7, number FROM numbers(500000);
INSERT INTO t_aio_shuffle_budget SELECT 8, number FROM numbers(500000);

SELECT k, sum(v) FROM t_aio_shuffle_budget GROUP BY k FORMAT Null
SETTINGS max_threads = 8, optimize_aggregation_in_order = 1, aggregation_in_order_shuffle = 1,
         aggregation_in_order_shuffle_max_buffered_bytes = 1; -- { serverError TOO_MANY_ROWS_OR_BYTES }

-- With the default (generous) budget the same query must succeed and be correct.
SELECT
    (SELECT groupBitXor(cityHash64(k, s)) FROM (SELECT k, sum(v) s FROM t_aio_shuffle_budget GROUP BY k)
        SETTINGS optimize_aggregation_in_order = 1, aggregation_in_order_shuffle = 1, max_threads = 8)
  = (SELECT groupBitXor(cityHash64(k, s)) FROM (SELECT k, sum(v) s FROM t_aio_shuffle_budget GROUP BY k)
        SETTINGS optimize_aggregation_in_order = 0, max_threads = 8);

-- The setting is UInt64, so values above Int64::max must remain a generous budget instead of wrapping negative.
SELECT
    (SELECT groupBitXor(cityHash64(k, s)) FROM (SELECT k, sum(v) s FROM t_aio_shuffle_budget GROUP BY k)
        SETTINGS optimize_aggregation_in_order = 1, aggregation_in_order_shuffle = 1, max_threads = 8,
                 aggregation_in_order_shuffle_max_buffered_bytes = 9223372036854775808)
  = (SELECT groupBitXor(cityHash64(k, s)) FROM (SELECT k, sum(v) s FROM t_aio_shuffle_budget GROUP BY k)
        SETTINGS optimize_aggregation_in_order = 0, max_threads = 8);

DROP TABLE t_aio_shuffle_budget;
