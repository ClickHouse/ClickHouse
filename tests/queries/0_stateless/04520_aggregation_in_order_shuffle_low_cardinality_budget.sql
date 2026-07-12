-- Regression test for the shuffle buffer budget with `LowCardinality` input. `ColumnLowCardinality::scatter`
-- keeps a single shared dictionary across all shard chunks, so the budget must charge that dictionary as the
-- stage-owned memory it is (once) rather than counting it on every scattered chunk. Accounting per-chunk
-- `allocatedBytes()` counted the shared dictionary on each queued chunk, which inflated the counter by many
-- times the real memory and made `aggregation_in_order_shuffle_max_buffered_bytes` throw
-- `TOO_MANY_ROWS_OR_BYTES` spuriously on safe queries. This uses `Chunk::bytes()` (`IColumn::byteSize`), which
-- excludes a shared dictionary, so the counter reflects the bytes the stage actually owns.

SET enable_parallel_replicas = 0;

-- The shuffle is disabled when `max_rows_to_group_by` is set (see 04514). The stateless-test profile sets a
-- huge `max_rows_to_group_by` by default, which would disable the shuffle for the whole test, so reset it to 0.
SET max_rows_to_group_by = 0;

DROP TABLE IF EXISTS t_aio_shuffle_lc;

-- A `LowCardinality(String)` column with a large per-block dictionary (many distinct long values). Parts that
-- each contain a single GROUP BY key are the worst case for buffering (see 04515): a scatter has to read its
-- whole part into one shard's queue while that shard's merge is blocked, so many scattered chunks - each
-- referencing the shared dictionary - are queued at once.
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

-- The stage owns a single shared dictionary (~a few MiB) plus the per-shard index/key buffers (tens of MiB at
-- most, bounded by the whole input), so a 64 MiB budget is comfortable. It must NOT throw. When the shared
-- dictionary was counted per queued chunk the inflated counter exceeded even hundreds of MiB here and the
-- query threw `TOO_MANY_ROWS_OR_BYTES`.
SELECT k, max(s) FROM t_aio_shuffle_lc GROUP BY k FORMAT Null
SETTINGS max_threads = 8, optimize_aggregation_in_order = 1, aggregation_in_order_shuffle = 1,
         aggregation_in_order_shuffle_max_buffered_bytes = 67108864;

-- Result parity with ordinary aggregation-in-order under the same budget.
SELECT
    (SELECT groupBitXor(cityHash64(k, m)) FROM (SELECT k, max(s) m FROM t_aio_shuffle_lc GROUP BY k)
        SETTINGS optimize_aggregation_in_order = 1, aggregation_in_order_shuffle = 1, max_threads = 8,
                 aggregation_in_order_shuffle_max_buffered_bytes = 67108864)
  = (SELECT groupBitXor(cityHash64(k, m)) FROM (SELECT k, max(s) m FROM t_aio_shuffle_lc GROUP BY k)
        SETTINGS optimize_aggregation_in_order = 0, max_threads = 8);

-- The budget still applies to `LowCardinality` input: a tiny cap must fail the query.
SELECT k, max(s) FROM t_aio_shuffle_lc GROUP BY k FORMAT Null
SETTINGS max_threads = 8, optimize_aggregation_in_order = 1, aggregation_in_order_shuffle = 1,
         aggregation_in_order_shuffle_max_buffered_bytes = 1; -- { serverError TOO_MANY_ROWS_OR_BYTES }

DROP TABLE t_aio_shuffle_lc;
