-- End-to-end test for shuffled aggregation-in-order over a `LowCardinality` aggregate argument and its
-- buffer budget. `ColumnLowCardinality::scatter` shares one dictionary across all shard chunks, so the
-- buffer budget must account for it correctly; the exact per-block dictionary accounting (counted once,
-- neither dropped nor multiplied by `num_shards`) is verified deterministically by the unit test
-- `BufferedShardByHashTransform.LowCardinalityDictionaryChargedOncePerBlock`. This test only checks the
-- pieces that do not depend on how far the pipeline reads ahead at runtime: that the shuffle is used for a
-- `LowCardinality` argument, that the budget is enforced at all, and that the result matches ordinary
-- aggregation-in-order.

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

DROP TABLE IF EXISTS t_aio_shuffle_lc;
CREATE TABLE t_aio_shuffle_lc (k UInt32, s LowCardinality(String)) ENGINE = MergeTree ORDER BY k;

-- Several single-key parts (merges stopped) so the in-order read produces more than one stream and the
-- shuffle is used.
SYSTEM STOP MERGES t_aio_shuffle_lc;
INSERT INTO t_aio_shuffle_lc SELECT 1, concat('v', toString(number % 500)) FROM numbers(30000);
INSERT INTO t_aio_shuffle_lc SELECT 2, concat('v', toString(number % 500)) FROM numbers(30000);
INSERT INTO t_aio_shuffle_lc SELECT 3, concat('v', toString(number % 500)) FROM numbers(30000);
INSERT INTO t_aio_shuffle_lc SELECT 4, concat('v', toString(number % 500)) FROM numbers(30000);
INSERT INTO t_aio_shuffle_lc SELECT 5, concat('v', toString(number % 500)) FROM numbers(30000);
INSERT INTO t_aio_shuffle_lc SELECT 6, concat('v', toString(number % 500)) FROM numbers(30000);
INSERT INTO t_aio_shuffle_lc SELECT 7, concat('v', toString(number % 500)) FROM numbers(30000);
INSERT INTO t_aio_shuffle_lc SELECT 8, concat('v', toString(number % 500)) FROM numbers(30000);

-- The shuffle path must actually be used with a `LowCardinality` aggregate argument in the stream.
SELECT countIf(explain LIKE '%BufferedShardByHashTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT k, max(s) FROM t_aio_shuffle_lc GROUP BY k
      SETTINGS max_threads = 8, optimize_aggregation_in_order = 1, aggregation_in_order_shuffle = 1);

-- The budget is enforced: a one-byte cap cannot hold even the first chunk each scatter reads ahead, so the
-- query must throw regardless of scheduling.
SELECT k, max(s) FROM t_aio_shuffle_lc GROUP BY k FORMAT Null
SETTINGS max_threads = 8, optimize_aggregation_in_order = 1,
         aggregation_in_order_shuffle = 1,
         aggregation_in_order_shuffle_max_buffered_bytes = 1; -- { serverError TOO_MANY_ROWS_OR_BYTES }

-- Correctness: the shuffle result must match ordinary aggregation-in-order.
SELECT
    (SELECT groupBitXor(cityHash64(k, m)) FROM (SELECT k, max(s) m FROM t_aio_shuffle_lc GROUP BY k)
        SETTINGS optimize_aggregation_in_order = 1, aggregation_in_order_shuffle = 1, max_threads = 8)
  = (SELECT groupBitXor(cityHash64(k, m)) FROM (SELECT k, max(s) m FROM t_aio_shuffle_lc GROUP BY k)
        SETTINGS optimize_aggregation_in_order = 0, max_threads = 8);

DROP TABLE t_aio_shuffle_lc;
