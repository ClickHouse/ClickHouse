-- Regression test for a quadratic slowdown (effectively a hang) in aggregation-in-order.
--
-- With `optimize_aggregation_in_order = 1` and a multi-key `GROUP BY` that uses the serialized
-- aggregation method (here: a `UInt64` key plus a `String` key), the "prealloc serialized" method
-- re-serialized the keys of the whole block on every sorting-key-prefix sub-range processed by
-- `Aggregator::executeOnBlockSmall`. A block with many distinct sorting-key prefixes therefore cost
-- `O(distinct_prefixes * block_rows)`, which made this query run for minutes and trip the
-- stress-test hung-check. The in-order path must stay linear.
--
-- This regression is a CPU-time blowup with no other observable footprint (the results are
-- identical and the memory/allocation profile is the same), so the guard must be a time limit. A
-- wall-clock limit (`max_execution_time`) is not usable here: earlier revisions of this test flaked
-- repeatedly on the debug flaky check because the query's wall-clock is dominated by how loaded the
-- CI runner is (a linear query on a heavily oversubscribed box can take longer than a quadratic one
-- on an idle box), not by whether the code is linear. Instead we assert on the query's CPU time
-- (`ProfileEvents['OSCPUVirtualTimeMicroseconds']`), read back from `system.query_log`: CPU time
-- measures the cycles the query actually consumed and is unaffected by box contention, so it
-- separates the linear code (a fraction of a CPU-second) from the quadratic code (tens of
-- CPU-seconds on release, well over a minute on the instrumented debug/sanitizer builds that run
-- this suite) by more than an order of magnitude regardless of runner load. The 3-second threshold
-- clears the linear path with a wide margin on every build and is far below the quadratic runtime,
-- so it catches a regression without any risk of a false failure on the fixed code.

DROP TABLE IF EXISTS t_agg_in_order_serialized;

CREATE TABLE t_agg_in_order_serialized (k1 UInt64, k2 String, v UInt64)
ENGINE = MergeTree ORDER BY k1;

-- Every row has a distinct k1, so the block is split into as many sorting-key-prefix
-- sub-ranges as there are rows - this is what triggered the quadratic behaviour.
INSERT INTO t_agg_in_order_serialized
SELECT number, toString(number % 8), number
FROM numbers(500000);

-- The in-order aggregation must produce the same result as regular hash aggregation.
SELECT
(
    SELECT groupBitXor(cityHash64(k1, k2, s))
    FROM (SELECT k1, k2, sum(v) AS s FROM t_agg_in_order_serialized GROUP BY k1, k2)
    SETTINGS optimize_aggregation_in_order = 1, optimize_read_in_order = 1,
             max_threads = 1, max_block_size = 16384
)
=
(
    SELECT groupBitXor(cityHash64(k1, k2, s))
    FROM (SELECT k1, k2, sum(v) AS s FROM t_agg_in_order_serialized GROUP BY k1, k2)
    SETTINGS optimize_aggregation_in_order = 0
);

-- Run the in-order aggregation once on its own (the `CPUGUARD` marker lets us find it in the log)
-- so its CPU time can be asserted on in isolation.
SELECT k1, k2, sum(v) FROM t_agg_in_order_serialized GROUP BY k1, k2 -- CPUGUARD
SETTINGS optimize_aggregation_in_order = 1, optimize_read_in_order = 1,
         max_threads = 1, max_block_size = 16384, log_queries = 1
FORMAT Null;

SYSTEM FLUSH LOGS query_log;

-- The in-order path must stay linear: its CPU time is a fraction of a second, far below the
-- 3-second guard, while the quadratic regression burns tens of CPU-seconds (much more on debug).
SELECT ProfileEvents['OSCPUVirtualTimeMicroseconds'] < 3000000
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
  AND current_database = currentDatabase()
  AND query LIKE '%CPUGUARD%' AND query NOT LIKE '%query_log%'
  AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC
LIMIT 1;

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
             max_threads = 4, max_block_size = 16384
)
=
(
    SELECT groupBitXor(cityHash64(k1, k2, s))
    FROM (SELECT k1, k2, sum(v) AS s FROM t_agg_in_order_serialized_multi GROUP BY k1, k2)
    SETTINGS optimize_aggregation_in_order = 0
);

DROP TABLE t_agg_in_order_serialized_multi;
