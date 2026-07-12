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
-- an expensive read; the max_execution_time guard fails fast and deterministically if the quadratic
-- behaviour ever comes back (with the fix the query runs in a fraction of a second, while a
-- regression blows well past the guard).

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
             max_threads = 1, max_block_size = 16384, max_execution_time = 20
)
=
(
    SELECT groupBitXor(cityHash64(k1, k2, s))
    FROM (SELECT k1, k2, sum(v) AS s FROM t_agg_in_order_serialized GROUP BY k1, k2)
    SETTINGS optimize_aggregation_in_order = 0
);

DROP TABLE t_agg_in_order_serialized;
