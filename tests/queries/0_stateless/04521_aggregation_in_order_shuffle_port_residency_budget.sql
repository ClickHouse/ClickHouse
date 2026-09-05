-- End-to-end test for shuffled aggregation-in-order and its buffer budget on a plain (non-shared) column.
-- The exact buffer accounting - in particular that a chunk pushed to an output port stays charged until the
-- downstream merge pulls it out of the port, rather than being released when it leaves the scatter's internal
-- queue - is verified deterministically by the unit test
-- `BufferedShardByHashTransform.PortResidentChunksChargedUntilConsumed`. This test only checks the pieces
-- that do not depend on how far the pipeline reads ahead at runtime: that the shuffle is used, that the
-- budget is enforced at all, and that the result matches ordinary aggregation-in-order.

SET enable_parallel_replicas = 0;
SET read_in_order_use_virtual_row = 0;

-- The shuffle is disabled when `max_rows_to_group_by` is set (see 04514). The stateless-test profile sets a
-- huge `max_rows_to_group_by` by default, which would disable the shuffle (and its buffer budget) for the
-- whole test, so reset it to 0.
SET max_rows_to_group_by = 0;

-- Read every part in its own in-order stream regardless of size, so the shuffle (which needs more than one
-- input stream) is applied without depending on the data volume crossing the concurrent-read thresholds.
SET merge_tree_min_rows_for_concurrent_read = 0;
SET merge_tree_min_bytes_for_concurrent_read = 0;

DROP TABLE IF EXISTS t_aio_shuffle_ports;
CREATE TABLE t_aio_shuffle_ports (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;

-- Several single-key parts (merges stopped) so the in-order read produces more than one stream and the
-- shuffle is used.
SYSTEM STOP MERGES t_aio_shuffle_ports;
INSERT INTO t_aio_shuffle_ports SELECT 1, number FROM numbers(30000);
INSERT INTO t_aio_shuffle_ports SELECT 2, number FROM numbers(30000);
INSERT INTO t_aio_shuffle_ports SELECT 3, number FROM numbers(30000);
INSERT INTO t_aio_shuffle_ports SELECT 4, number FROM numbers(30000);
INSERT INTO t_aio_shuffle_ports SELECT 5, number FROM numbers(30000);
INSERT INTO t_aio_shuffle_ports SELECT 6, number FROM numbers(30000);
INSERT INTO t_aio_shuffle_ports SELECT 7, number FROM numbers(30000);
INSERT INTO t_aio_shuffle_ports SELECT 8, number FROM numbers(30000);

-- The shuffle path must actually be used.
SELECT countIf(explain LIKE '%BufferedShardByHashTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT k, sum(v) FROM t_aio_shuffle_ports GROUP BY k
      SETTINGS max_threads = 8, optimize_aggregation_in_order = 1, aggregation_in_order_shuffle = 1);

-- The budget is enforced: a one-byte cap cannot hold even the first chunk each scatter reads ahead, so the
-- query must throw regardless of scheduling.
SELECT k, sum(v) FROM t_aio_shuffle_ports GROUP BY k FORMAT Null
SETTINGS max_threads = 8, optimize_aggregation_in_order = 1,
         aggregation_in_order_shuffle = 1,
         aggregation_in_order_shuffle_max_buffered_bytes = 1; -- { serverError TOO_MANY_ROWS_OR_BYTES }

-- Correctness: the shuffle result must match ordinary aggregation-in-order.
SELECT
    (SELECT groupBitXor(cityHash64(k, s)) FROM (SELECT k, sum(v) s FROM t_aio_shuffle_ports GROUP BY k)
        SETTINGS optimize_aggregation_in_order = 1, aggregation_in_order_shuffle = 1, max_threads = 8)
  = (SELECT groupBitXor(cityHash64(k, s)) FROM (SELECT k, sum(v) s FROM t_aio_shuffle_ports GROUP BY k)
        SETTINGS optimize_aggregation_in_order = 0, max_threads = 8);

DROP TABLE t_aio_shuffle_ports;
