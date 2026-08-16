-- End-to-end test for shuffled aggregation-in-order over `AggregateFunction` columns, whose states live in
-- arenas the column only keeps alive rather than owns. `ColumnAggregateFunction::allocatedBytes` cannot size
-- that correctly - it counts an owned arena in full and ignores a foreign one entirely - so the buffer budget
-- registers each arena as a shared object of its own. That the arena is charged exactly once, and neither
-- dropped nor charged once per shard view, is verified deterministically by the unit test
-- `BufferedShardByHashTransform.AggregateFunctionArenaChargedOncePerBlock`. This test only checks the pieces
-- that do not depend on how far the pipeline reads ahead at runtime: that the shuffle is used on such a query,
-- that the default budget does not reject it, that the budget is enforced at all, and that the result matches
-- ordinary aggregation-in-order.

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

DROP TABLE IF EXISTS t_aio_shuffle_state;
CREATE TABLE t_aio_shuffle_state (k UInt64, s AggregateFunction(groupArray, UInt64))
ENGINE = AggregatingMergeTree ORDER BY k;

-- Several single-key parts (merges stopped) so the in-order read produces more than one stream and the
-- shuffle is used. Every state accumulates its elements in an arena, so the arenas - not the columns - hold
-- the bulk of what the shuffle buffers.
SYSTEM STOP MERGES t_aio_shuffle_state;
INSERT INTO t_aio_shuffle_state SELECT 1, groupArrayState(number) FROM numbers(20000);
INSERT INTO t_aio_shuffle_state SELECT 2, groupArrayState(number) FROM numbers(20000);
INSERT INTO t_aio_shuffle_state SELECT 3, groupArrayState(number) FROM numbers(20000);
INSERT INTO t_aio_shuffle_state SELECT 4, groupArrayState(number) FROM numbers(20000);
INSERT INTO t_aio_shuffle_state SELECT 5, groupArrayState(number) FROM numbers(20000);
INSERT INTO t_aio_shuffle_state SELECT 6, groupArrayState(number) FROM numbers(20000);
INSERT INTO t_aio_shuffle_state SELECT 7, groupArrayState(number) FROM numbers(20000);
INSERT INTO t_aio_shuffle_state SELECT 8, groupArrayState(number) FROM numbers(20000);

-- The shuffle path must actually be used for an aggregate-state argument.
SELECT countIf(explain LIKE '%BufferedShardByHashTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT k, length(groupArrayMerge(s)) FROM t_aio_shuffle_state GROUP BY k
      SETTINGS max_threads = 8, optimize_aggregation_in_order = 1, aggregation_in_order_shuffle = 1);

-- The default budget must not reject the query: the arenas the shard views share are charged once, not once
-- per shard view, so nothing here comes anywhere near `aggregation_in_order_shuffle_max_buffered_bytes`.
SELECT k, length(groupArrayMerge(s)) FROM t_aio_shuffle_state GROUP BY k FORMAT Null
SETTINGS max_threads = 8, optimize_aggregation_in_order = 1, aggregation_in_order_shuffle = 1;

-- The budget is enforced: a one-byte cap cannot hold even the first chunk each scatter reads ahead, so the
-- query must throw regardless of scheduling.
SELECT k, length(groupArrayMerge(s)) FROM t_aio_shuffle_state GROUP BY k FORMAT Null
SETTINGS max_threads = 8, optimize_aggregation_in_order = 1,
         aggregation_in_order_shuffle = 1,
         aggregation_in_order_shuffle_max_buffered_bytes = 1; -- { serverError TOO_MANY_ROWS_OR_BYTES }

-- Correctness: the shuffle result must match ordinary aggregation-in-order.
SELECT
    (SELECT groupBitXor(cityHash64(k, n)) FROM (SELECT k, length(groupArrayMerge(s)) n FROM t_aio_shuffle_state GROUP BY k)
        SETTINGS optimize_aggregation_in_order = 1, aggregation_in_order_shuffle = 1, max_threads = 8)
  = (SELECT groupBitXor(cityHash64(k, n)) FROM (SELECT k, length(groupArrayMerge(s)) n FROM t_aio_shuffle_state GROUP BY k)
        SETTINGS optimize_aggregation_in_order = 0, max_threads = 8);

DROP TABLE t_aio_shuffle_state;
