-- Regression test for the shuffle buffer budget on a generic variable-length column (`String`). The budget
-- charges the exact bytes resident after `scatter`: the per-shard owned bytes of every buffered shard chunk,
-- plus each shared dictionary once (see `BufferedShardByHashTransform::generateOutputChunks`). This tracks the
-- real memory of the buffered chunks - including buffers that `scatter` regrows per shard (`ColumnString` does
-- not reserve `chars`, so each shard rebuilds its own `chars` buffer) - which charging the pre-split block
-- could under-count. `String` input must therefore trip a tight `aggregation_in_order_shuffle_max_buffered_bytes`
-- and must not spuriously trip it on well-distributed input.

SET enable_parallel_replicas = 0;

-- The shuffle is disabled when `max_rows_to_group_by` is set (see 04514). The stateless-test profile sets a
-- huge `max_rows_to_group_by` by default, which would disable the shuffle (and its buffer budget) for the
-- whole test, so reset it to 0.
SET max_rows_to_group_by = 0;

-- One part per INSERT (a single-chunk part escapes into the per-shard merge's initialization and contributes
-- nothing to the buffered floor - see 04521), and keep every part read as one plain in-order stream.
SET max_insert_threads = 1;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;
SET read_in_order_two_level_merge_threshold = 100;

DROP TABLE IF EXISTS t_aio_shuffle_str;

CREATE TABLE t_aio_shuffle_str (k UInt8, s String) ENGINE = MergeTree ORDER BY k;

SYSTEM STOP MERGES t_aio_shuffle_str;
-- Parts that each hold one long single-key run are the worst case for buffering (see 04515/04521): a
-- per-shard sorted merge can finish a lane only after the scatters of the other parts reach EOF, so a scatter
-- has to read most of its part into one shard while that shard's merge is blocked. ~100-byte strings make the
-- `chars` buffers dominate the charged bytes.
INSERT INTO t_aio_shuffle_str SELECT 1, concat(repeat('x', 100), toString(number)) FROM numbers(50000);
INSERT INTO t_aio_shuffle_str SELECT 2, concat(repeat('x', 100), toString(number)) FROM numbers(50000);
INSERT INTO t_aio_shuffle_str SELECT 3, concat(repeat('x', 100), toString(number)) FROM numbers(50000);
INSERT INTO t_aio_shuffle_str SELECT 4, concat(repeat('x', 100), toString(number)) FROM numbers(50000);
INSERT INTO t_aio_shuffle_str SELECT 5, concat(repeat('x', 100), toString(number)) FROM numbers(50000);
INSERT INTO t_aio_shuffle_str SELECT 6, concat(repeat('x', 100), toString(number)) FROM numbers(50000);
INSERT INTO t_aio_shuffle_str SELECT 7, concat(repeat('x', 100), toString(number)) FROM numbers(50000);
INSERT INTO t_aio_shuffle_str SELECT 8, concat(repeat('x', 100), toString(number)) FROM numbers(50000);

-- The shuffle path must actually be used with a `String` aggregate argument in the stream.
SELECT countIf(explain LIKE '%BufferedShardByHashTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT k, max(s) FROM t_aio_shuffle_str GROUP BY k
      SETTINGS max_threads = 8, optimize_aggregation_in_order = 1, aggregation_in_order_shuffle = 1);

-- With small chunks (`max_block_size` = 8192) each part spans several chunks, so far more than the merge's
-- first chunk per lane stays buffered: with 8 parts of 50000 rows the buffered `String` data is tens of MiB
-- and far exceeds a 16 MiB budget, so the query must throw. `max_threads`/`max_block_size` are pinned so the
-- shape does not depend on the harness's random settings.
SELECT k, max(s) FROM t_aio_shuffle_str GROUP BY k FORMAT Null
SETTINGS max_threads = 8, max_block_size = 8192, optimize_aggregation_in_order = 1,
         aggregation_in_order_shuffle = 1,
         aggregation_in_order_shuffle_max_buffered_bytes = 16777216; -- { serverError TOO_MANY_ROWS_OR_BYTES }

-- A tiny cap must fail as well.
SELECT k, max(s) FROM t_aio_shuffle_str GROUP BY k FORMAT Null
SETTINGS max_threads = 8, max_block_size = 8192, optimize_aggregation_in_order = 1,
         aggregation_in_order_shuffle = 1,
         aggregation_in_order_shuffle_max_buffered_bytes = 1; -- { serverError TOO_MANY_ROWS_OR_BYTES }

DROP TABLE t_aio_shuffle_str;

-- Correctness and no spurious trip on well-distributed `String` input. Evenly spread keys keep every shard's
-- merge fed, so the queues stay short and the buffered bytes stay far below the default cap - the exact
-- post-split accounting must not trip it on safe queries. The shuffle result must match ordinary
-- aggregation-in-order.
DROP TABLE IF EXISTS t_aio_shuffle_str_wide;
CREATE TABLE t_aio_shuffle_str_wide (k UInt32, s String) ENGINE = MergeTree ORDER BY k;
SYSTEM STOP MERGES t_aio_shuffle_str_wide;
INSERT INTO t_aio_shuffle_str_wide SELECT number, concat(repeat('y', 100), toString(number)) FROM numbers(20000);
INSERT INTO t_aio_shuffle_str_wide SELECT number + 20000, concat(repeat('y', 100), toString(number)) FROM numbers(20000);

SELECT
    (SELECT groupBitXor(cityHash64(k, m)) FROM (SELECT k, max(s) m FROM t_aio_shuffle_str_wide GROUP BY k)
        SETTINGS optimize_aggregation_in_order = 1, aggregation_in_order_shuffle = 1, max_threads = 8)
  = (SELECT groupBitXor(cityHash64(k, m)) FROM (SELECT k, max(s) m FROM t_aio_shuffle_str_wide GROUP BY k)
        SETTINGS optimize_aggregation_in_order = 0, max_threads = 8);

DROP TABLE t_aio_shuffle_str_wide;
