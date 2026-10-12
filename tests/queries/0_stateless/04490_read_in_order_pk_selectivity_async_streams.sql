-- Tags: no-random-settings, no-random-merge-tree-settings
-- Regression test for the read parallelism the PK-selectivity guard recovers.
-- With asynchronous reads (`allow_asynchronous_read_from_io_pool_for_merge_tree = 1`) the read
-- step is intentionally bumped to several read streams while the *output* is resized down to
-- `output_streams_limit` (= `max_streams_for_merge_tree_reading`) only to bound memory. The guard
-- must look at the read parallelism (`requested_num_streams`), not the output width: with
-- `max_streams_for_merge_tree_reading = 1` and `max_threads > 1` the read still happens with
-- multiple streams, so a poor-PK `ORDER BY` must fall back to parallel reading plus a full sort.
-- Comparing against the output cap (`1`) would hide that parallelism and wrongly keep the slow
-- single-stream in-order plan that this guard exists to avoid.

DROP TABLE IF EXISTS t_read_in_order_pk_async;

-- Small index_granularity to produce enough marks for the guard to fire (it requires
-- total_marks > read streams), and several parts to mirror the multi-part motivating case.
CREATE TABLE t_read_in_order_pk_async (path String, value UInt64)
ENGINE = MergeTree ORDER BY path
SETTINGS index_granularity = 64, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES t_read_in_order_pk_async;

INSERT INTO t_read_in_order_pk_async SELECT concat('path/', toString(number % 1000), '/file.log'), number FROM numbers(0, 25000);
INSERT INTO t_read_in_order_pk_async SELECT concat('path/', toString(number % 1000), '/file.log'), number FROM numbers(25000, 25000);
INSERT INTO t_read_in_order_pk_async SELECT concat('path/', toString(number % 1000), '/file.log'), number FROM numbers(50000, 25000);

SET enable_parallel_replicas = 0;
SET allow_asynchronous_read_from_io_pool_for_merge_tree = 1, max_streams_for_merge_tree_reading = 1;

-- Multi-stream read parallelism (`max_threads = 4`) but output capped to one stream. The guard
-- must still see real read parallelism and fall back to a full sort (`PartialSortingTransform`).
-- Before the fix, the guard compared against the output cap (`1`) and kept read-in-order.
SELECT 'async_multistream_poor_pk_full_sort';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT * FROM t_read_in_order_pk_async
    WHERE path LIKE '%file.log' ORDER BY path
    SETTINGS max_threads = 4, read_in_order_max_primary_key_ratio = 0.0
) WHERE explain LIKE '%PartialSortingTransform%';

-- Disabling the guard (ratio = 1.0) keeps read-in-order even with the same read parallelism.
SELECT 'async_multistream_setting_off_keeps_in_order';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT * FROM t_read_in_order_pk_async
    WHERE path LIKE '%file.log' ORDER BY path
    SETTINGS max_threads = 4, read_in_order_max_primary_key_ratio = 1.0
) WHERE explain LIKE '%PartialSortingTransform%';

-- Genuine single read stream (`max_threads = 1`): there is no parallelism to recover, so the
-- guard must NOT fire regardless of poor selectivity (disabling read-in-order would only add a
-- sort on top of the same single stream). This pins that the guard checks the real read width.
SELECT 'async_single_stream_keeps_in_order';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT * FROM t_read_in_order_pk_async
    WHERE path LIKE '%file.log' ORDER BY path
    SETTINGS max_threads = 1, read_in_order_max_primary_key_ratio = 0.0
) WHERE explain LIKE '%PartialSortingTransform%';

-- Results must be correct (sorted, complete) when the guard fires on the async path.
SELECT 'correctness';
SELECT count(), min(path) = 'path/0/file.log' FROM (
    SELECT path FROM t_read_in_order_pk_async
    WHERE path LIKE '%file.log' ORDER BY path
    SETTINGS max_threads = 4, read_in_order_max_primary_key_ratio = 0.0
);

DROP TABLE t_read_in_order_pk_async;
