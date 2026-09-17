-- A merge must not consume a chunk that has no rows. An input can finish its port while the chunk it
-- just pushed is still in flight, so a merging transform can pull a chunk with no rows from an input
-- that already reads as finished. Here `BufferChunksTransform` is the producer. The tail of every
-- partition holds only empty arrays, so the last chunk it pushes is the `0 rows / N columns` shape
-- `ARRAY JOIN` builds, and the merge is still busy with the other three partitions when the port
-- finishes.

DROP TABLE IF EXISTS t_read_in_order_3;

CREATE TABLE t_read_in_order_3 (p UInt8, k UInt32, v UInt32, vals Array(UInt32))
ENGINE = ReplacingMergeTree(v) PARTITION BY p ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 8;

-- Keep two parts per partition so the per-partition `FINAL` merge is a real merge.
SYSTEM STOP MERGES t_read_in_order_3;

INSERT INTO t_read_in_order_3 SELECT number % 4, number, 1, if(number >= 300, [], [toUInt32(number)]) FROM numbers(400);
INSERT INTO t_read_in_order_3 SELECT number % 4, number, 2, if(number >= 300, [], [toUInt32(number)]) FROM numbers(400);

-- The row count and the threshold are both multiples of the partition count, so every partition holds
-- the same number of rows with values and the same number without. An uneven split staggers where the
-- partitions run out of values, and the empty final chunk stops coinciding with the port finishing.
SELECT uniqExact(with_values) = 1, uniqExact(without_values) = 1
FROM (SELECT p, countIf(notEmpty(vals)) AS with_values, countIf(empty(vals)) AS without_values
      FROM t_read_in_order_3 FINAL GROUP BY p);

-- Consuming the rowless chunk is `Logical error: 'max_rows > 0'` in debug and sanitizer builds. Every
-- setting below is load-bearing, and the query must run bare: an enclosing aggregate lets
-- `query_plan_remove_redundant_sorting` drop the sort, and with it the merge the rowless chunk has to
-- reach.
SELECT k, x FROM t_read_in_order_3 FINAL ARRAY JOIN vals AS x ORDER BY k
SETTINGS do_not_merge_across_partitions_select_final = 1, optimize_read_in_order = 1,
    read_in_order_use_buffering = 1, max_threads = 1, max_block_size = 1
FORMAT Null;

-- A mis-merge drops rows, so the counts move. `query_plan_remove_redundant_sorting = 0` keeps the
-- inner sort, without which the merge is not in the plan at all.
SELECT count(), sum(k), sum(x) FROM
(
    SELECT k, x FROM t_read_in_order_3 FINAL ARRAY JOIN vals AS x ORDER BY k
)
SETTINGS do_not_merge_across_partitions_select_final = 1, optimize_read_in_order = 1,
    read_in_order_use_buffering = 1, max_threads = 1, max_block_size = 1,
    query_plan_remove_redundant_sorting = 0;

-- Each query above only reaches the merge while the plan still puts buffering in front of a sorted
-- merge, and the results are correct either way, so pin the shape of each rather than assume it.
-- Every SETTINGS clause sits on the outermost statement, matching the query it pins: one attached to
-- an inner subquery loses to the same setting arriving as a client option.
SELECT countIf(explain LIKE '%BufferChunks%') > 0, countIf(explain LIKE '%MergingSortedTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT k, x FROM t_read_in_order_3 FINAL ARRAY JOIN vals AS x ORDER BY k
      SETTINGS do_not_merge_across_partitions_select_final = 1, optimize_read_in_order = 1,
          read_in_order_use_buffering = 1, max_threads = 1, max_block_size = 1);

SELECT countIf(explain LIKE '%BufferChunks%') > 0, countIf(explain LIKE '%MergingSortedTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT count(), sum(k), sum(x) FROM
      (
          SELECT k, x FROM t_read_in_order_3 FINAL ARRAY JOIN vals AS x ORDER BY k
      )
      SETTINGS do_not_merge_across_partitions_select_final = 1, optimize_read_in_order = 1,
          read_in_order_use_buffering = 1, max_threads = 1, max_block_size = 1,
          query_plan_remove_redundant_sorting = 0);

DROP TABLE t_read_in_order_3;
