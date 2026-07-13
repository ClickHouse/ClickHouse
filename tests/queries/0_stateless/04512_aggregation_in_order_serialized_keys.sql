-- Regression test for a quadratic slowdown (effectively a hang) in aggregation-in-order.
--
-- With optimize_aggregation_in_order = 1 and a multi-key GROUP BY that uses the serialized
-- aggregation method (here: a UInt64 key plus a String key), the "prealloc serialized" method
-- re-serialized the keys of the whole block on every sorting-key-prefix sub-range processed by
-- Aggregator::executeOnBlockSmall. A block with many distinct sorting-key prefixes therefore
-- cost O(distinct_prefixes * block_rows), which made this query run for minutes and trip the
-- stress-test hung-check. The in-order path must stay linear, so this must finish quickly.
--
-- The in-order aggregation is pinned to a fixed, small configuration (max_threads = 1,
-- max_block_size = 16384, optimize_read_in_order = 1) so the flaky check cannot randomize it into
-- an expensive read. The max_execution_time limit is a guard against the quadratic blowup, not a
-- tight assertion on the linear runtime, and the two runtimes are orders of magnitude apart: on a
-- release build the linear path takes a fraction of a second while the quadratic one takes tens of
-- seconds; on a debug or sanitizer build the linear path can still take tens of seconds under the
-- parallel load of the flaky check (a 20-second limit flaked here for exactly that reason) while the
-- quadratic one runs for many minutes. A 120-second limit clears the linear path with a wide margin
-- on every build and is still far below the quadratic runtime on the instrumented builds that run
-- this suite, so it catches a regression without any risk of a false timeout on the fixed code.

DROP TABLE IF EXISTS t_agg_in_order_serialized;

CREATE TABLE t_agg_in_order_serialized (k1 UInt64, k2 String, v UInt64)
ENGINE = MergeTree ORDER BY k1;

-- Every row has a distinct k1, so the block is split into as many sorting-key-prefix
-- sub-ranges as there are rows - this is what triggered the quadratic behaviour.
INSERT INTO t_agg_in_order_serialized
SELECT number, toString(number % 8), number
FROM numbers(200000);

-- The in-order aggregation must produce the same result as regular hash aggregation.
SELECT
(
    SELECT groupBitXor(cityHash64(k1, k2, s))
    FROM (SELECT k1, k2, sum(v) AS s FROM t_agg_in_order_serialized GROUP BY k1, k2)
    SETTINGS optimize_aggregation_in_order = 1, optimize_read_in_order = 1,
             max_threads = 1, max_block_size = 16384, max_execution_time = 120
)
=
(
    SELECT groupBitXor(cityHash64(k1, k2, s))
    FROM (SELECT k1, k2, sum(v) AS s FROM t_agg_in_order_serialized GROUP BY k1, k2)
    SETTINGS optimize_aggregation_in_order = 0
);

DROP TABLE t_agg_in_order_serialized;

-- The multi-stream in-order pipeline: when the table is sorted by the full GROUP BY key and is
-- read in several streams, the per-stream in-order results are merged by
-- MergingAggregatedBucketTransform via Aggregator::mergeBlocks - a whole-block path that shares
-- the method choice with the small-block path above and must stay correct with it.

DROP TABLE IF EXISTS t_agg_in_order_serialized_multi;

CREATE TABLE t_agg_in_order_serialized_multi (k1 UInt64, k2 String, v UInt64)
ENGINE = MergeTree ORDER BY (k1, k2);

SYSTEM STOP MERGES t_agg_in_order_serialized_multi;

-- Four parts with identical key ranges, so every group appears in every read stream and the
-- final merge has real work to do.
INSERT INTO t_agg_in_order_serialized_multi SELECT number, toString(number % 16), number FROM numbers(50000);
INSERT INTO t_agg_in_order_serialized_multi SELECT number, toString(number % 16), number * 2 FROM numbers(50000);
INSERT INTO t_agg_in_order_serialized_multi SELECT number, toString(number % 16), number * 3 FROM numbers(50000);
INSERT INTO t_agg_in_order_serialized_multi SELECT number, toString(number % 16), number * 4 FROM numbers(50000);

-- The multi-stream merge must actually be present in the pipeline.
SELECT count() > 0 FROM
(
    EXPLAIN PIPELINE
    SELECT k1, k2, sum(v) FROM t_agg_in_order_serialized_multi GROUP BY k1, k2
    SETTINGS optimize_aggregation_in_order = 1, optimize_read_in_order = 1,
             max_threads = 4, max_block_size = 16384
)
WHERE explain LIKE '%MergingAggregatedBucketTransform%';

-- And it must produce the same result as regular hash aggregation.
SELECT
(
    SELECT groupBitXor(cityHash64(k1, k2, s))
    FROM (SELECT k1, k2, sum(v) AS s FROM t_agg_in_order_serialized_multi GROUP BY k1, k2)
    SETTINGS optimize_aggregation_in_order = 1, optimize_read_in_order = 1,
             max_threads = 4, max_block_size = 16384, max_execution_time = 120
)
=
(
    SELECT groupBitXor(cityHash64(k1, k2, s))
    FROM (SELECT k1, k2, sum(v) AS s FROM t_agg_in_order_serialized_multi GROUP BY k1, k2)
    SETTINGS optimize_aggregation_in_order = 0
);

DROP TABLE t_agg_in_order_serialized_multi;
