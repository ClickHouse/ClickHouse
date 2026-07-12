-- Regression test for the shuffle buffer budget with `LowCardinality` input. `ColumnLowCardinality::scatter`
-- keeps a single dictionary shared across all shard chunks, and `ColumnLowCardinality::byteSize` reports zero
-- for a shared dictionary, so charging the budget per queued shard chunk with `Chunk::bytes()` dropped that
-- dictionary from the counter entirely - every buffered block kept a multi-MiB dictionary alive that the
-- budget did not see. The budget must instead account per input block, charging the whole pre-split block
-- (via `Chunk::allocatedBytes()`) once and holding the charge until the block's last shard chunk drains, so a
-- shared dictionary is counted exactly once per buffered block (neither dropped nor multiplied by `num_shards`).

SET enable_parallel_replicas = 0;

-- The shuffle is disabled when `max_rows_to_group_by` is set (see 04514). The stateless-test profile sets a
-- huge `max_rows_to_group_by` by default, which would disable the shuffle (and its buffer budget) for the
-- whole test, so reset it to 0.
SET max_rows_to_group_by = 0;

DROP TABLE IF EXISTS t_aio_shuffle_lc;

-- A `LowCardinality(String)` column with a large per-block dictionary (many distinct long values). Parts that
-- each contain a single GROUP BY key are the worst case for buffering (see 04515): a scatter has to read its
-- whole part into one shard's queue while that shard's merge is blocked, so many scattered chunks - each
-- keeping the shared dictionary alive - are queued at once.
CREATE TABLE t_aio_shuffle_lc (k UInt8, s LowCardinality(String)) ENGINE = MergeTree ORDER BY k;

SYSTEM STOP MERGES t_aio_shuffle_lc;
INSERT INTO t_aio_shuffle_lc SELECT 1, concat(repeat('x', 500), toString(number % 10000)) FROM numbers(500000);
INSERT INTO t_aio_shuffle_lc SELECT 2, concat(repeat('x', 500), toString(number % 10000)) FROM numbers(500000);
INSERT INTO t_aio_shuffle_lc SELECT 3, concat(repeat('x', 500), toString(number % 10000)) FROM numbers(500000);
INSERT INTO t_aio_shuffle_lc SELECT 4, concat(repeat('x', 500), toString(number % 10000)) FROM numbers(500000);
INSERT INTO t_aio_shuffle_lc SELECT 5, concat(repeat('x', 500), toString(number % 10000)) FROM numbers(500000);
INSERT INTO t_aio_shuffle_lc SELECT 6, concat(repeat('x', 500), toString(number % 10000)) FROM numbers(500000);
INSERT INTO t_aio_shuffle_lc SELECT 7, concat(repeat('x', 500), toString(number % 10000)) FROM numbers(500000);
INSERT INTO t_aio_shuffle_lc SELECT 8, concat(repeat('x', 500), toString(number % 10000)) FROM numbers(500000);

-- The shuffle path must actually be used with a `LowCardinality` aggregate argument in the stream.
SELECT countIf(explain LIKE '%BufferedShardByHashTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT k, max(s) FROM t_aio_shuffle_lc GROUP BY k
      SETTINGS max_threads = 8, optimize_aggregation_in_order = 1, aggregation_in_order_shuffle = 1);

-- The dictionary IS charged against the budget. Each buffered block keeps a multi-MiB `LowCardinality`
-- dictionary alive, so with the dictionary counted once per block the buffered bytes far exceed a 64 MiB
-- budget and the query must throw. When the shared dictionary was dropped from the budget (counted as zero
-- owned bytes) only the ~16 MiB of index columns were charged and this 64 MiB budget wrongly passed.
SELECT k, max(s) FROM t_aio_shuffle_lc GROUP BY k FORMAT Null
SETTINGS max_threads = 8, max_block_size = 65536, optimize_aggregation_in_order = 1,
         aggregation_in_order_shuffle = 1,
         aggregation_in_order_shuffle_max_buffered_bytes = 67108864; -- { serverError TOO_MANY_ROWS_OR_BYTES }

-- A tiny cap must fail as well.
SELECT k, max(s) FROM t_aio_shuffle_lc GROUP BY k FORMAT Null
SETTINGS max_threads = 8, max_block_size = 65536, optimize_aggregation_in_order = 1,
         aggregation_in_order_shuffle = 1,
         aggregation_in_order_shuffle_max_buffered_bytes = 1; -- { serverError TOO_MANY_ROWS_OR_BYTES }

DROP TABLE t_aio_shuffle_lc;

-- Correctness and no spurious trip on well-distributed `LowCardinality` input. Evenly spread keys keep every
-- shard's merge fed, so the queues stay short and the (correctly accounted, one-dictionary-per-block) budget
-- stays far below the default cap - counting the dictionary once per block must not trip it on safe queries.
-- The shuffle result must match ordinary aggregation-in-order.
DROP TABLE IF EXISTS t_aio_shuffle_lc_wide;
CREATE TABLE t_aio_shuffle_lc_wide (k UInt32, s LowCardinality(String)) ENGINE = MergeTree ORDER BY k;
SYSTEM STOP MERGES t_aio_shuffle_lc_wide;
INSERT INTO t_aio_shuffle_lc_wide SELECT number, concat(repeat('y', 100), toString(number % 1000)) FROM numbers(500000);
INSERT INTO t_aio_shuffle_lc_wide SELECT number + 500000, concat(repeat('y', 100), toString(number % 1000)) FROM numbers(500000);

SELECT
    (SELECT groupBitXor(cityHash64(k, m)) FROM (SELECT k, max(s) m FROM t_aio_shuffle_lc_wide GROUP BY k)
        SETTINGS optimize_aggregation_in_order = 1, aggregation_in_order_shuffle = 1, max_threads = 8)
  = (SELECT groupBitXor(cityHash64(k, m)) FROM (SELECT k, max(s) m FROM t_aio_shuffle_lc_wide GROUP BY k)
        SETTINGS optimize_aggregation_in_order = 0, max_threads = 8);

DROP TABLE t_aio_shuffle_lc_wide;
