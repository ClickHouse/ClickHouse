-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/106237
-- `BufferedShardByHashTransform` deadlocked the pipeline when a downstream `ConcatProcessor`
-- (here produced by narrowing a `UNION ALL` with `max_streams_for_union_step` < pipeline width)
-- activated its inputs sequentially and the data hashed to only a subset of shards.
-- The empty-queue output ports were never finished, so `Concat` waited forever on an empty
-- branch while the chunks queued on the loaded shards could never drain -> `Pipeline stuck`.
-- The three low-cardinality keys guarantee the skew that triggers the stuck state.
--
-- Every query below pins two settings, because the stateless profile would otherwise stop
-- the sharded transform from being built at all and the test would pass vacuously:
--   * `max_rows_to_group_by = 0` - the profile sets it to 10G
--     (tests/config/users.d/limits.yaml) and `AggregatingStep::canUseShardedAggregation`
--     rejects any nonzero value.
--   * `enable_parallel_replicas = 0` - the profile turns parallel replicas on for plain
--     MergeTree tables (tests/config/users.d/enable_parallel_replicas.xml), and the
--     replica-side plan replaces the local pipeline this test needs. Pinning the setting
--     rather than tagging `no-parallel-replicas` keeps the test running in every job.
-- Measured with EXPLAIN PIPELINE: the transform is present twice with both pins, and zero
-- times if either one is dropped.

DROP TABLE IF EXISTS test_106237;
CREATE TABLE test_106237 (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO test_106237 SELECT 0 AS a, number AS b FROM numbers(100000);
INSERT INTO test_106237 SELECT 1 AS a, number AS b FROM numbers(100000);
INSERT INTO test_106237 SELECT 2 AS a, number AS b FROM numbers(100000);

-- max_threads must be high enough that the sharded pipeline opens several shard outputs:
-- with the skewed keys above, a wider fan-out guarantees the sequentially-activated
-- ConcatProcessor demands an empty shard, which is what triggers the stuck state. A small
-- value (e.g. 2-3) routes all keys onto the demanded shards and hides the bug.
-- Precondition: both halves of the deadlock topology are present for this shape - the
-- sharded transform, and the `ConcatProcessor` that narrowing the `UNION ALL` produces.
-- Asserting only the transform is not enough: with narrowing disabled the transform is
-- still built but no `Concat` appears, and the queries below then pass with the fix
-- reverted because the sequential consumer they need is gone.
SELECT countIf(explain LIKE '%BufferedShardByHash%') > 0 AND countIf(explain LIKE '%Concat%') > 0
FROM (
    EXPLAIN PIPELINE
    SELECT a, max(s)
    FROM (
        SELECT a, sum(b) AS s FROM test_106237 GROUP BY a
        UNION ALL
        SELECT a, sum(b) AS s FROM test_106237 GROUP BY a
    )
    GROUP BY a
    ORDER BY a
    SETTINGS enable_sharding_aggregator = 1, max_threads = 16,
             max_streams_for_union_step = 1, max_rows_to_group_by = 0,
             enable_parallel_replicas = 0
);

SELECT a, max(s)
FROM (
    SELECT a, sum(b) AS s FROM test_106237 GROUP BY a
    UNION ALL
    SELECT a, sum(b) AS s FROM test_106237 GROUP BY a
)
GROUP BY a
ORDER BY a
SETTINGS enable_sharding_aggregator = 1,
         max_threads = 16,
         max_streams_for_union_step = 1,
         max_rows_to_group_by = 0,
         enable_parallel_replicas = 0;

-- Same deadlock via a scalar-subquery-wrapped UNION ALL (found by the AST-fuzzer oracle,
-- see the issue thread). The stuck state is a property of the sharded transform, not of
-- where the UNION ALL sits, so wrapping it in a scalar subquery reaches the same code path.
-- Precondition: the same topology holds for the scalar-subquery shape.
SELECT countIf(explain LIKE '%BufferedShardByHash%') > 0 AND countIf(explain LIKE '%Concat%') > 0
FROM (
    EXPLAIN PIPELINE
    SELECT count() FROM (
        SELECT a, sum(b) AS s FROM test_106237 GROUP BY a
        UNION ALL
        SELECT a, sum(b) AS s FROM test_106237 GROUP BY a
    )
    SETTINGS enable_sharding_aggregator = 1, max_threads = 16,
             max_streams_for_union_step = 1, max_rows_to_group_by = 0,
             enable_parallel_replicas = 0
);

SELECT (
    SELECT count() FROM (
        SELECT a, sum(b) AS s FROM test_106237 GROUP BY a
        UNION ALL
        SELECT a, sum(b) AS s FROM test_106237 GROUP BY a
    ) SETTINGS enable_sharding_aggregator = 1,
              max_threads = 16,
              max_streams_for_union_step = 1,
              max_rows_to_group_by = 0,
              enable_parallel_replicas = 0
);

-- Cover the soft-cap paths in `BufferedShardByHashTransform::prepare`. The two queries
-- above keep every shard queue at 3 chunks or fewer, so `MAX_QUEUE_LENGTH` is never
-- reached and neither cap branch runs. A smaller `max_block_size` over a larger table
-- makes one sharding transform accumulate enough chunks to reach both.
--
-- `max_block_size` is 1000 rather than a smaller value on purpose. Queue depth scales
-- with the number of chunks, but it only has to clear `MAX_QUEUE_LENGTH`, whereas the
-- per-chunk hashing and scattering cost scales with the chunk count too. Measured peak
-- depth here is 99 for (a) and 12-70 for (b) against a cap of 10, so both branches run
-- with a wide margin, while a tenfold smaller `max_block_size` only buys more depth at
-- roughly 2-3x the CPU. `max_threads = 16` is load-bearing and must not be lowered:
-- with fewer shards the consumer keeps up and peak depth never exceeds 1.

-- (a) Cap reached while a sibling port has an empty queue and downstream demand:
--     the bypass keeps pulling input instead of back-pressuring, which is what the
--     deadlock fix requires. A single key sends every row to one shard.
--
-- Reaching the cap needs 16 shards fed faster than one consumer drains them, so both cap
-- cases pin what the fan-out depends on: `index_granularity` and `min_bytes_for_wide_part`
-- on the table, plus the four `merge_tree_*_for_concurrent_read` settings per query. The
-- randomized values shrink either the read-stream count or the shard queue depth below the
-- cap. Over the randomized grid the pins keep both branches firing in every cell; dropping
-- them silently stops at least one branch in 11 of 18 cells, so the preconditions below
-- assert presence only and cannot replace the pins.
DROP TABLE IF EXISTS test_106237_cap;
CREATE TABLE test_106237_cap (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0;
INSERT INTO test_106237_cap SELECT 7 AS a, number AS b FROM numbers(1000000);

-- Precondition: the bypass case needs both halves of the topology, exactly like the two
-- deadlock queries above - the sharded transform and the sequential `ConcatProcessor`.
SELECT countIf(explain LIKE '%BufferedShardByHash%') > 0 AND countIf(explain LIKE '%Concat%') > 0
FROM (
    EXPLAIN PIPELINE
    SELECT a, max(s)
    FROM (
        SELECT a, sum(b) AS s FROM test_106237_cap GROUP BY a
        UNION ALL
        SELECT a, sum(b) AS s FROM test_106237_cap GROUP BY a
    )
    GROUP BY a
    ORDER BY a
    SETTINGS enable_sharding_aggregator = 1, max_threads = 16,
             max_streams_for_union_step = 1, max_block_size = 1000,
             max_rows_to_group_by = 0, enable_parallel_replicas = 0,
             merge_tree_min_rows_for_concurrent_read = 1,
             merge_tree_min_bytes_for_concurrent_read = 1,
             merge_tree_min_rows_for_concurrent_read_for_remote_filesystem = 1,
             merge_tree_min_bytes_for_concurrent_read_for_remote_filesystem = 1
);

SELECT a, max(s)
FROM (
    SELECT a, sum(b) AS s FROM test_106237_cap GROUP BY a
    UNION ALL
    SELECT a, sum(b) AS s FROM test_106237_cap GROUP BY a
)
GROUP BY a
ORDER BY a
SETTINGS enable_sharding_aggregator = 1,
         max_threads = 16,
         max_streams_for_union_step = 1,
         max_block_size = 1000,
         max_rows_to_group_by = 0,
         enable_parallel_replicas = 0,
         merge_tree_min_rows_for_concurrent_read = 1,
         merge_tree_min_bytes_for_concurrent_read = 1,
         merge_tree_min_rows_for_concurrent_read_for_remote_filesystem = 1,
         merge_tree_min_bytes_for_concurrent_read_for_remote_filesystem = 1;

-- (b) Cap reached while NO port has an empty queue: distinct keys spread rows over every
--     shard, so no shard queue is ever empty. In this state the back-pressure return that
--     fires is the no-progress guard above the cap check, not the cap check's own PortFull
--     arm - a queue at capacity is by definition non-empty, so that arm is unreachable and
--     measures zero. This case therefore drives the cap and pins correctness; it cannot
--     assert the back-pressure itself, because the cap is a memory bound with no
--     query-visible effect (removing it only changes peak queue depth, which no SQL
--     oracle exposes).
DROP TABLE IF EXISTS test_106237_spread;
CREATE TABLE test_106237_spread (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0;
INSERT INTO test_106237_spread SELECT number AS a, number AS b FROM numbers(1000000);

-- Precondition: only the transform, because this shape has no `UNION ALL` to narrow and
-- therefore no `ConcatProcessor` - the back-pressure branch does not need one.
SELECT countIf(explain LIKE '%BufferedShardByHash%') > 0
FROM (
    EXPLAIN PIPELINE
    SELECT count(), sum(s)
    FROM (SELECT a, sum(b) AS s FROM test_106237_spread GROUP BY a)
    SETTINGS enable_sharding_aggregator = 1, max_threads = 16, max_block_size = 1000,
             max_rows_to_group_by = 0, enable_parallel_replicas = 0,
             merge_tree_min_rows_for_concurrent_read = 1,
             merge_tree_min_bytes_for_concurrent_read = 1,
             merge_tree_min_rows_for_concurrent_read_for_remote_filesystem = 1,
             merge_tree_min_bytes_for_concurrent_read_for_remote_filesystem = 1
);

SELECT count(), sum(s)
FROM (SELECT a, sum(b) AS s FROM test_106237_spread GROUP BY a)
SETTINGS enable_sharding_aggregator = 1,
         max_threads = 16,
         max_block_size = 1000,
         max_rows_to_group_by = 0,
         enable_parallel_replicas = 0,
         merge_tree_min_rows_for_concurrent_read = 1,
         merge_tree_min_bytes_for_concurrent_read = 1,
         merge_tree_min_rows_for_concurrent_read_for_remote_filesystem = 1,
         merge_tree_min_bytes_for_concurrent_read_for_remote_filesystem = 1;

DROP TABLE test_106237_spread;
DROP TABLE test_106237_cap;
DROP TABLE test_106237;
