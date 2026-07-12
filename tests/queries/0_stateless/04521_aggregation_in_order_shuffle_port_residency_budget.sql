-- Regression test for the shuffle buffer budget accounting of chunks that have left the scatter's internal
-- queue but are still parked in an output port on the way to the per-shard merge. A chunk pushed to an
-- `OutputPort` stays resident in the port state until the downstream merge pulls it (`OutputPort::hasData()`
-- is still true), so releasing its budget charge the moment it leaves the queue let those bytes escape
-- `aggregation_in_order_shuffle_max_buffered_bytes`: a scatter could park a full block in each of its ports
-- while the shared counter read far less than the memory actually held. The charge must instead stay held
-- until the merge pulls the chunk out of the port.
--
-- Long single-key runs are the worst case (see 04515): a scatter reads its part into one shard while that
-- shard's merge is blocked, so at a large `max_block_size` most of the buffered data sits as big chunks in
-- flight between the scatter and the merge (in the output ports) rather than in the queue. A budget that the
-- correctly-accounted buffered bytes exceed - but that the queue-only accounting stayed under - must fail the
-- query. With the charge released too early (on dequeue) this 6 MiB budget wrongly passed.

SET enable_parallel_replicas = 0;

-- The shuffle is disabled when `max_rows_to_group_by` is set (see 04514). The stateless-test profile sets a
-- huge `max_rows_to_group_by` by default, which would disable the shuffle (and its buffer budget) for the
-- whole test, so reset it to 0.
SET max_rows_to_group_by = 0;

DROP TABLE IF EXISTS t_aio_shuffle_ports;

CREATE TABLE t_aio_shuffle_ports (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;

SYSTEM STOP MERGES t_aio_shuffle_ports;
INSERT INTO t_aio_shuffle_ports SELECT 1, number FROM numbers(100000);
INSERT INTO t_aio_shuffle_ports SELECT 2, number FROM numbers(100000);
INSERT INTO t_aio_shuffle_ports SELECT 3, number FROM numbers(100000);
INSERT INTO t_aio_shuffle_ports SELECT 4, number FROM numbers(100000);
INSERT INTO t_aio_shuffle_ports SELECT 5, number FROM numbers(100000);
INSERT INTO t_aio_shuffle_ports SELECT 6, number FROM numbers(100000);
INSERT INTO t_aio_shuffle_ports SELECT 7, number FROM numbers(100000);
INSERT INTO t_aio_shuffle_ports SELECT 8, number FROM numbers(100000);

-- The shuffle path must actually be used.
SELECT countIf(explain LIKE '%BufferedShardByHashTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT k, sum(v) FROM t_aio_shuffle_ports GROUP BY k
      SETTINGS max_threads = 8, optimize_aggregation_in_order = 1, aggregation_in_order_shuffle = 1);

-- With a large `max_block_size` the buffered chunks are big and mostly held in the output ports (few, large
-- chunks) rather than in the queue, so the correctly-accounted buffered bytes far exceed 6 MiB and the query
-- must throw. Counting only the queued chunks (releasing port-resident chunks too early) kept the counter
-- below 6 MiB, so this budget wrongly passed. `max_threads`/`max_block_size` are pinned so the shape does not
-- depend on the harness's random settings.
SELECT k, sum(v) FROM t_aio_shuffle_ports GROUP BY k FORMAT Null
SETTINGS max_threads = 8, max_block_size = 65536, optimize_aggregation_in_order = 1,
         aggregation_in_order_shuffle = 1,
         aggregation_in_order_shuffle_max_buffered_bytes = 6291456; -- { serverError TOO_MANY_ROWS_OR_BYTES }

-- A tiny cap must fail as well.
SELECT k, sum(v) FROM t_aio_shuffle_ports GROUP BY k FORMAT Null
SETTINGS max_threads = 8, max_block_size = 65536, optimize_aggregation_in_order = 1,
         aggregation_in_order_shuffle = 1,
         aggregation_in_order_shuffle_max_buffered_bytes = 1; -- { serverError TOO_MANY_ROWS_OR_BYTES }

-- With the default (generous) budget the same query must succeed and match ordinary aggregation-in-order.
SELECT
    (SELECT groupBitXor(cityHash64(k, s)) FROM (SELECT k, sum(v) s FROM t_aio_shuffle_ports GROUP BY k)
        SETTINGS optimize_aggregation_in_order = 1, aggregation_in_order_shuffle = 1, max_threads = 8)
  = (SELECT groupBitXor(cityHash64(k, s)) FROM (SELECT k, sum(v) s FROM t_aio_shuffle_ports GROUP BY k)
        SETTINGS optimize_aggregation_in_order = 0, max_threads = 8);

DROP TABLE t_aio_shuffle_ports;
