-- Regression test for the shuffle buffer budget accounting of chunks that have left the scatter's internal
-- queue but are still parked in an output port on the way to the per-shard merge. A chunk pushed to an
-- `OutputPort` stays resident in the port state until the downstream merge pulls it (`OutputPort::hasData()`
-- is still true), so releasing its budget charge the moment it leaves the queue let those bytes escape
-- `aggregation_in_order_shuffle_max_buffered_bytes`: a scatter could park a full block in each of its ports
-- while the shared counter read far less than the memory actually held. The charge must instead stay held
-- until the merge pulls the chunk out of the port (including after the scatter's input is exhausted: the
-- charge survives until the merge actually consumes the parked chunk, not until the scatter finishes).
--
-- Why the budget trip is deterministic: every part holds one long single-key run, so a per-shard merge gets
-- data on the lane fed by that part and neither data nor EOF on the lanes from the other scatters until those
-- scatters exhaust their inputs. A merge absorbs at most ONE chunk per lane while it initializes (that chunk
-- leaves the port and its charge is legitimately released) and cannot pull anything more until every lane has
-- data or EOF, i.e. until the last scatter finishes reading. So at that moment everything beyond the first
-- chunk of each part is still buffered in the scatter stage and charged: with 8 parts of 150000 rows and
-- chunks capped at `max_block_size` = 65536, at least 8 * (150000 - 65536) * 16 bytes ~ 10.8 MB is charged
-- simultaneously, which crosses the 6 MiB budget no matter how the threads interleave.

SET enable_parallel_replicas = 0;

-- The shuffle is disabled when `max_rows_to_group_by` is set (see 04514). The stateless-test profile sets a
-- huge `max_rows_to_group_by` by default, which would disable the shuffle (and its buffer budget) for the
-- whole test, so reset it to 0.
SET max_rows_to_group_by = 0;

-- One part per INSERT. With parallel insert threads each INSERT splits into several single-chunk parts, and a
-- single-chunk part contributes nothing to the guaranteed buffered floor above: its whole content is the
-- "first chunk" that escapes into the per-shard merge's initialization, so the budget would never be crossed.
SET max_insert_threads = 1;

-- Keep every part read as one plain in-order stream. Randomized range splitting or two-level in-order merging
-- would split a part among several streams, and each sub-stream's first chunk escapes into the merges'
-- initialization the same way, eroding the guaranteed buffered floor.
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;
SET read_in_order_two_level_merge_threshold = 100;

DROP TABLE IF EXISTS t_aio_shuffle_ports;

CREATE TABLE t_aio_shuffle_ports (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;

SYSTEM STOP MERGES t_aio_shuffle_ports;
INSERT INTO t_aio_shuffle_ports SELECT 1, number FROM numbers(150000);
INSERT INTO t_aio_shuffle_ports SELECT 2, number FROM numbers(150000);
INSERT INTO t_aio_shuffle_ports SELECT 3, number FROM numbers(150000);
INSERT INTO t_aio_shuffle_ports SELECT 4, number FROM numbers(150000);
INSERT INTO t_aio_shuffle_ports SELECT 5, number FROM numbers(150000);
INSERT INTO t_aio_shuffle_ports SELECT 6, number FROM numbers(150000);
INSERT INTO t_aio_shuffle_ports SELECT 7, number FROM numbers(150000);
INSERT INTO t_aio_shuffle_ports SELECT 8, number FROM numbers(150000);

-- The shuffle path must actually be used.
SELECT countIf(explain LIKE '%BufferedShardByHashTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT k, sum(v) FROM t_aio_shuffle_ports GROUP BY k
      SETTINGS max_threads = 8, optimize_aggregation_in_order = 1, aggregation_in_order_shuffle = 1);

-- With a large `max_block_size` the buffered chunks are big and mostly held in the output ports (few, large
-- chunks) rather than in the queue, so the correctly-accounted buffered bytes exceed 6 MiB (see the floor
-- estimate at the top) and the query must throw. Counting only the queued chunks (releasing port-resident
-- chunks too early) kept the counter below 6 MiB, so this budget wrongly passed. `max_threads`/`max_block_size`
-- are pinned so the shape does not depend on the harness's random settings.
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
