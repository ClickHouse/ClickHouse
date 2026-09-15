-- `read_in_order_use_virtual_row` is enabled by default, but virtual rows cannot be
-- planned when a fixed leading primary-key column is omitted from the GROUP BY key.
-- The shuffle must use the actual pipeline state rather than the global setting.
SET enable_parallel_replicas = 0;
SET max_rows_to_group_by = 0;
SET merge_tree_min_rows_for_concurrent_read = 0;
SET merge_tree_min_bytes_for_concurrent_read = 0;

DROP TABLE IF EXISTS t_aio_shuffle_fixed_prefix;
CREATE TABLE t_aio_shuffle_fixed_prefix (a UInt64, b UInt64, v UInt64)
ENGINE = MergeTree ORDER BY (a, b) SETTINGS index_granularity = 128;

SYSTEM STOP MERGES t_aio_shuffle_fixed_prefix;
INSERT INTO t_aio_shuffle_fixed_prefix SELECT 1, number % 5000, number FROM numbers(50000);
INSERT INTO t_aio_shuffle_fixed_prefix SELECT 1, number % 5000, number FROM numbers(50000);
INSERT INTO t_aio_shuffle_fixed_prefix SELECT 1, number % 5000, number FROM numbers(50000);

-- `a = 1` makes the matched `b` key non-contiguous. `optimizeReadInOrder` disables
-- virtual rows for this shape, so the shuffle is safe and must still be planned.
SELECT countIf(explain LIKE '%BufferedShardByHashTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT b, sum(v) FROM t_aio_shuffle_fixed_prefix WHERE a = 1 GROUP BY b
      SETTINGS max_threads = 4, optimize_aggregation_in_order = 1, aggregation_in_order_shuffle = 1);

-- The same fixed-prefix shape must keep using the shuffle when the profile enables
-- `read_in_order_use_virtual_row_per_block`: no virtual rows are planned for it, so there
-- is no stream metadata to preserve. The gate has to look at the pipeline, not the setting.
SELECT countIf(explain LIKE '%BufferedShardByHashTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT b, sum(v) FROM t_aio_shuffle_fixed_prefix WHERE a = 1 GROUP BY b
      SETTINGS max_threads = 4, optimize_aggregation_in_order = 1, aggregation_in_order_shuffle = 1,
               read_in_order_use_virtual_row_per_block = 1);

-- Per-block virtual rows are emitted directly by `MergeTreeSource`, without a
-- `VirtualRowTransform` in the pipeline, and the gate checks that carrier as well. They are only
-- planned for a read-in-order `Sorting` step, so a plain `GROUP BY` over the whole primary key does
-- not get them either and keeps the reshuffle even with the setting on.
SELECT countIf(explain LIKE '%BufferedShardByHashTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT a, b, sum(v) FROM t_aio_shuffle_fixed_prefix GROUP BY a, b
      SETTINGS max_threads = 4, optimize_aggregation_in_order = 1, aggregation_in_order_shuffle = 1,
               read_in_order_use_virtual_row_per_block = 1);

SELECT
    (SELECT groupBitXor(cityHash64(b, s))
     FROM (SELECT b, sum(v) s FROM t_aio_shuffle_fixed_prefix WHERE a = 1 GROUP BY b)
     SETTINGS max_threads = 4, optimize_aggregation_in_order = 1, aggregation_in_order_shuffle = 1)
  = (SELECT groupBitXor(cityHash64(b, s))
     FROM (SELECT b, sum(v) s FROM t_aio_shuffle_fixed_prefix WHERE a = 1 GROUP BY b)
     SETTINGS max_threads = 4, optimize_aggregation_in_order = 0);

DROP TABLE t_aio_shuffle_fixed_prefix;
