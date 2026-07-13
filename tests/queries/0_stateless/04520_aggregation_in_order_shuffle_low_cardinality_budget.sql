-- Regression test for the shuffle buffer budget accounting of a `LowCardinality` dictionary.
-- `ColumnLowCardinality::scatter` keeps a single dictionary shared across all shard chunks, and
-- `ColumnLowCardinality::byteSize` reports zero for a shared dictionary. Charging the budget per queued shard
-- chunk with `Chunk::bytes()` therefore dropped the dictionary from the counter entirely: every buffered
-- block kept a multi-hundred-KiB dictionary alive that the budget never saw, so a query holding many buffered
-- blocks could exceed `aggregation_in_order_shuffle_max_buffered_bytes` many times over without tripping it.
-- The budget instead accounts per input block, charging each shard chunk's owned bytes plus the shared
-- dictionary once and holding the charge until the block's last shard chunk drains, so a shared dictionary is
-- counted exactly once per buffered block (neither dropped nor multiplied by `num_shards`).
--
-- Why the budget trip is deterministic (see 04521 for the general argument): every part is one long single-key
-- run, so a per-shard merge gets data on the lane fed by that part and neither data nor EOF on the lanes from
-- the other scatters until those scatters exhaust their inputs. A merge absorbs at most one chunk per lane
-- while it initializes and then cannot pull anything more until the last scatter finishes reading, so at that
-- moment every chunk beyond the first of each part is still buffered in the scatter stage and charged. With 8
-- parts and chunks capped at `max_block_size` = 32768, several chunks per part stay buffered at once; each
-- keeps a ~1 MiB dictionary alive, so the correctly-accounted buffered bytes reach far past a 20 MiB budget
-- (measured peak ~40 MiB) and the query must throw. With the dictionary dropped from the budget only the small
-- index columns of those chunks are charged and the buffered bytes stay near 8 MiB, well under 20 MiB, so the
-- query wrongly succeeds - that is the regression this test catches.

SET enable_parallel_replicas = 0;

-- The shuffle is disabled when `max_rows_to_group_by` is set (see 04514). The stateless-test profile sets a
-- huge `max_rows_to_group_by` by default, which would disable the shuffle (and its buffer budget) for the
-- whole test, so reset it to 0.
SET max_rows_to_group_by = 0;

-- One part per INSERT: with parallel insert threads an INSERT can split into several single-chunk parts, and a
-- single-chunk part contributes nothing to the buffered floor (its whole content is the "first chunk" that
-- escapes into the per-shard merge's initialization). Small parts are also read as one concatenated in-order
-- stream, which disables the shuffle entirely (it needs more than one input stream); large single-key parts
-- keep the shuffle on and the buffering deterministic.
SET max_insert_threads = 1;

-- Keep every part read as one plain in-order stream. Randomized range splitting or two-level in-order merging
-- would split a part among several sub-streams, and each sub-stream's first chunk escapes into the merges'
-- initialization the same way, eroding the buffered floor.
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;
SET read_in_order_two_level_merge_threshold = 100;

DROP TABLE IF EXISTS t_aio_shuffle_lc;

-- A `LowCardinality(String)` column with a large per-block dictionary (2000 distinct ~505-byte values,
-- ~1 MiB). Every part holds a single GROUP BY key so the buffering is the deterministic worst case above.
CREATE TABLE t_aio_shuffle_lc (k UInt8, s LowCardinality(String)) ENGINE = MergeTree ORDER BY k;

SYSTEM STOP MERGES t_aio_shuffle_lc;
INSERT INTO t_aio_shuffle_lc SELECT 1, concat(repeat('x', 500), toString(number % 2000)) FROM numbers(150000);
INSERT INTO t_aio_shuffle_lc SELECT 2, concat(repeat('x', 500), toString(number % 2000)) FROM numbers(150000);
INSERT INTO t_aio_shuffle_lc SELECT 3, concat(repeat('x', 500), toString(number % 2000)) FROM numbers(150000);
INSERT INTO t_aio_shuffle_lc SELECT 4, concat(repeat('x', 500), toString(number % 2000)) FROM numbers(150000);
INSERT INTO t_aio_shuffle_lc SELECT 5, concat(repeat('x', 500), toString(number % 2000)) FROM numbers(150000);
INSERT INTO t_aio_shuffle_lc SELECT 6, concat(repeat('x', 500), toString(number % 2000)) FROM numbers(150000);
INSERT INTO t_aio_shuffle_lc SELECT 7, concat(repeat('x', 500), toString(number % 2000)) FROM numbers(150000);
INSERT INTO t_aio_shuffle_lc SELECT 8, concat(repeat('x', 500), toString(number % 2000)) FROM numbers(150000);

-- The shuffle path must actually be used with a `LowCardinality` aggregate argument in the stream.
SELECT countIf(explain LIKE '%BufferedShardByHashTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT k, max(s) FROM t_aio_shuffle_lc GROUP BY k
      SETTINGS max_threads = 8, optimize_aggregation_in_order = 1, aggregation_in_order_shuffle = 1);

-- The dictionary IS charged against the budget. Many buffered blocks keep the ~1 MiB dictionary alive, so with
-- the dictionary counted once per block the buffered bytes reach ~40 MiB and far exceed a 20 MiB budget, so the
-- query must throw. When the shared dictionary was dropped from the budget only the small index columns of the
-- buffered chunks were charged (buffered bytes near 8 MiB) and this 20 MiB budget wrongly passed.
-- `max_threads`/`max_block_size` are pinned so the shape does not depend on the harness's random settings.
SELECT k, max(s) FROM t_aio_shuffle_lc GROUP BY k FORMAT Null
SETTINGS max_threads = 8, max_block_size = 32768, optimize_aggregation_in_order = 1,
         aggregation_in_order_shuffle = 1,
         aggregation_in_order_shuffle_max_buffered_bytes = 20971520; -- { serverError TOO_MANY_ROWS_OR_BYTES }

-- A tiny cap must fail as well.
SELECT k, max(s) FROM t_aio_shuffle_lc GROUP BY k FORMAT Null
SETTINGS max_threads = 8, max_block_size = 32768, optimize_aggregation_in_order = 1,
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
INSERT INTO t_aio_shuffle_lc_wide SELECT number, concat(repeat('y', 100), toString(number % 1000)) FROM numbers(20000);
INSERT INTO t_aio_shuffle_lc_wide SELECT number + 20000, concat(repeat('y', 100), toString(number % 1000)) FROM numbers(20000);

SELECT
    (SELECT groupBitXor(cityHash64(k, m)) FROM (SELECT k, max(s) m FROM t_aio_shuffle_lc_wide GROUP BY k)
        SETTINGS optimize_aggregation_in_order = 1, aggregation_in_order_shuffle = 1, max_threads = 8)
  = (SELECT groupBitXor(cityHash64(k, m)) FROM (SELECT k, max(s) m FROM t_aio_shuffle_lc_wide GROUP BY k)
        SETTINGS optimize_aggregation_in_order = 0, max_threads = 8);

DROP TABLE t_aio_shuffle_lc_wide;
