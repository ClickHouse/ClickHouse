-- Regression test for a quadratic blowup in aggregation in order that showed up as a
-- "Hung check failed, possible deadlock found" in the server-side AST fuzzer stress test.
--
-- When the data is sorted by a prefix of the GROUP BY keys (here `s`, while grouping by `s, n`),
-- `AggregatingInOrderTransform` aggregates each run of equal `s` with a hash table over the full
-- key. A multi-column numbers/strings key selects the `prealloc_serialized` aggregation method,
-- whose state serializes the whole input block up front on construction. Because a fresh state is
-- built for every run of equal `s` (one per row here), a single block became
-- O(number_of_runs * block_size), i.e. quadratic, and did not respect `max_execution_time` because
-- one `consume` call processes a whole block without yielding.
--
-- The fix makes the per-run in-order path use the plain `serialized` method (lazy, per-row key
-- serialization), restoring linear time. After the fix the query below finishes well within the
-- limit; before it, the per-block work is O(number_of_runs * block_size) and the query runs for
-- many minutes (it originally tripped the AST-fuzzer Hung check), so it exceeds `max_execution_time`
-- and throws.
--
-- The `max_execution_time` limit here is deliberately generous. It is a guard against the quadratic
-- blowup, not a tight assertion on the linear runtime, and the two runtimes are orders of magnitude
-- apart. On a release build the linear path takes a fraction of a second while the quadratic one
-- takes tens of seconds; on a sanitizer build the linear path can still take tens of seconds under
-- parallel load (a 20-second limit flaked here for exactly that reason) while the quadratic one runs
-- for many minutes. A 120-second limit clears the linear path with a wide margin on every build and
-- is still far below the quadratic runtime on the instrumented builds that run this suite (and on
-- which the fuzzer that first surfaced this bug runs), so it catches a regression without any risk
-- of a false timeout on the fixed code.

DROP TABLE IF EXISTS t_agg_in_order_serialized;
CREATE TABLE t_agg_in_order_serialized (s String, n UInt64) ENGINE = MergeTree ORDER BY s;

INSERT INTO t_agg_in_order_serialized SELECT toString(number), number FROM numbers(300000);

SELECT count() FROM (SELECT s, n FROM t_agg_in_order_serialized GROUP BY s, n)
SETTINGS optimize_aggregation_in_order = 1, optimize_read_in_order = 1,
         max_threads = 1, max_block_size = 16384, max_execution_time = 120;

DROP TABLE t_agg_in_order_serialized;

-- Multi-stream variant (`max_threads > 1`). Reading several parts in order produces more than one
-- stream, so the pipeline becomes:
--     AggregatingInOrderTransform -> FinishAggregatingInOrderTransform -> MergingAggregatedBucketTransform
-- The `serialized` fallback must be scoped to the per-run `AggregatingInOrderTransform` path only;
-- the whole-block `MergingAggregatedBucketTransform` merge (via `Aggregator::mergeBlocks`) keeps the
-- `prealloc_serialized` method, where it is a win. The value of this case is the correctness check
-- below: the multi-stream in-order result must be identical to regular aggregation, which would
-- break if the merge path were downgraded by mistake. The `max_execution_time` here is only a loose
-- backstop: with several parts read in parallel the per-run quadratic work is spread across streams,
-- so its wall-clock is not a reliable regression signal; the single-stream cases above and below are
-- the primary quadratic-time guards.

DROP TABLE IF EXISTS t_agg_in_order_serialized_mt;
CREATE TABLE t_agg_in_order_serialized_mt (s String, n UInt64) ENGINE = MergeTree ORDER BY s;
SYSTEM STOP MERGES t_agg_in_order_serialized_mt;

INSERT INTO t_agg_in_order_serialized_mt SELECT toString(number), number FROM numbers(0, 100000);
INSERT INTO t_agg_in_order_serialized_mt SELECT toString(number), number FROM numbers(100000, 100000);
INSERT INTO t_agg_in_order_serialized_mt SELECT toString(number), number FROM numbers(200000, 100000);

SELECT count() FROM (SELECT s, n FROM t_agg_in_order_serialized_mt GROUP BY s, n)
SETTINGS optimize_aggregation_in_order = 1, optimize_read_in_order = 1,
         max_threads = 4, max_block_size = 16384, max_execution_time = 120;

-- The multi-stream in-order result must be byte-for-byte identical to regular aggregation.
SELECT
(
    SELECT sum(cityHash64(s, n, c)) FROM
    (
        SELECT s, n, count() AS c FROM t_agg_in_order_serialized_mt GROUP BY s, n
        SETTINGS optimize_aggregation_in_order = 1, optimize_read_in_order = 1,
                 max_threads = 4, max_block_size = 16384
    )
)
=
(
    SELECT sum(cityHash64(s, n, c)) FROM
    (
        SELECT s, n, count() AS c FROM t_agg_in_order_serialized_mt GROUP BY s, n
        SETTINGS optimize_aggregation_in_order = 0, optimize_read_in_order = 0
    )
);

DROP TABLE t_agg_in_order_serialized_mt;

-- Nullable variant. A multi-column key `Nullable(String), Nullable(UInt64)` (a string plus a
-- number, so not all keys are fixed-size contiguous) selects the `nullable_prealloc_serialized`
-- method, which serializes keys through a different, null-map-aware path in `HashMethodSerialized`
-- than the non-nullable `prealloc_serialized` above. The quadratic blowup applies to that method
-- just the same, and the fix downgrades it to `nullable_serialized` on the per-run in-order path.
-- The data below carries actual NULL values in both keys (disjoint, so every `(s, n)` pair stays
-- distinct), so the null-map serialization is exercised.

DROP TABLE IF EXISTS t_agg_in_order_serialized_null;
CREATE TABLE t_agg_in_order_serialized_null (s Nullable(String), n Nullable(UInt64))
    ENGINE = MergeTree ORDER BY s SETTINGS allow_nullable_key = 1;

INSERT INTO t_agg_in_order_serialized_null
    SELECT if(number % 100 = 0, NULL, toString(number)),
           if(number % 100 = 1, NULL, number)
    FROM numbers(300000);

SELECT count() FROM (SELECT s, n FROM t_agg_in_order_serialized_null GROUP BY s, n)
SETTINGS optimize_aggregation_in_order = 1, optimize_read_in_order = 1,
         max_threads = 1, max_block_size = 16384, max_execution_time = 120;

-- The nullable in-order result must be identical to regular aggregation.
SELECT
(
    SELECT sum(cityHash64(s, n, c)) FROM
    (
        SELECT s, n, count() AS c FROM t_agg_in_order_serialized_null GROUP BY s, n
        SETTINGS optimize_aggregation_in_order = 1, optimize_read_in_order = 1,
                 max_threads = 1, max_block_size = 16384
    )
)
=
(
    SELECT sum(cityHash64(s, n, c)) FROM
    (
        SELECT s, n, count() AS c FROM t_agg_in_order_serialized_null GROUP BY s, n
        SETTINGS optimize_aggregation_in_order = 0, optimize_read_in_order = 0
    )
);

DROP TABLE t_agg_in_order_serialized_null;
