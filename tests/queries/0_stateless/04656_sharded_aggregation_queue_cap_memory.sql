-- Tags: long
-- The sharded aggregator keeps its per-shard queues bounded when a consumer is draining them,
-- so a GROUP BY whose key leaves most shards without rows does not buffer the whole scanned
-- input. Regression test for https://github.com/ClickHouse/ClickHouse/issues/116038.
--
-- The cap's own effect is peak queue depth, which no SQL oracle exposes; `max_memory_usage`
-- turns it into one, because queued chunks are tracked memory. What a queued chunk costs is
-- its bytes, not its rows, so the payload is wide and the row count small: bisected on this
-- shape, the query needs 39-43 MiB with the cap honoured and 259-290 MiB without it, and the
-- same aggregation off the sharded path needs 11 MiB. The 96 MiB budget below therefore sits
-- above both passing requirements and well under the failing one. Reducing the row count
-- further does not work: at 500k rows the two requirements cross over and the oracle inverts.
--
-- Pins, for the same reasons as 04342_sharded_aggregation_pipeline_stuck:
--   * `max_rows_to_group_by = 0` - the stateless profile sets it to 10G and
--     `AggregatingStep::canUseShardedAggregation` rejects any nonzero value.
--   * `enable_parallel_replicas = 0` - the profile enables parallel replicas for plain
--     MergeTree, and the replica-side plan replaces the local pipeline this test needs.
--   * `max_block_size` and `max_threads` are pinned because the fan-out and the queue depth
--     depend on them; the randomized values can drop the shard count below the point where
--     any queue reaches the cap. At `max_threads = 4` the four keys reach only shards 0 and
--     3, so two of the four queues stay empty - that skew is what the cap has to survive.
--   * `max_insert_threads` is pinned because the fixture's cost, not its content, depends on
--     it, and this test is re-run many times concurrently by the flaky check.

DROP TABLE IF EXISTS test_116038;
CREATE TABLE test_116038 (a UInt64, b String) ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0;
INSERT INTO test_116038 SELECT number % 4 AS a, repeat(hex(cityHash64(number)), 8) AS b
FROM numbers_mt(1000000) SETTINGS max_insert_threads = 1;

-- Precondition: the sharded transform is really in the pipeline at the width the skew is chosen
-- for, and no `ConcatProcessor` is involved - the cap bypass is reachable with a plain `Resize`
-- + `AggregatingTransform` consumer. The width is asserted, not just the presence: at a narrower
-- fan-out the four keys occupy every shard, no shard is left empty while demanded, and the arm
-- would pass without ever reaching the predicate.
SELECT countIf(explain LIKE '%BufferedShardByHashTransform × 4 1 → 4%') > 0,
       countIf(explain LIKE '%Concat%') = 0
FROM (
    EXPLAIN PIPELINE
    SELECT a, uniqCombined(b), uniqCombined(concat(b, '1')), uniqCombined(concat(b, '2'))
    FROM test_116038
    GROUP BY a
    SETTINGS enable_sharding_aggregator = 1, max_threads = 4, max_block_size = 1024,
             max_rows_to_group_by = 0, enable_parallel_replicas = 0
);

-- Control: the same aggregation without sharding fits the budget comfortably, so a failure
-- of the query below is attributable to the sharded path and not to the aggregation itself.
SELECT a, uniqCombined(b) > 0, uniqCombined(concat(b, '1')) > 0, uniqCombined(concat(b, '2')) > 0
FROM test_116038
GROUP BY a
ORDER BY a
SETTINGS enable_sharding_aggregator = 0, max_threads = 4, max_block_size = 1024,
         max_rows_to_group_by = 0, enable_parallel_replicas = 0,
         max_memory_usage = 100663296, max_bytes_ratio_before_external_group_by = 0;

SELECT a, uniqCombined(b) > 0, uniqCombined(concat(b, '1')) > 0, uniqCombined(concat(b, '2')) > 0
FROM test_116038
GROUP BY a
ORDER BY a
SETTINGS enable_sharding_aggregator = 1, max_threads = 4, max_block_size = 1024,
         max_rows_to_group_by = 0, enable_parallel_replicas = 0,
         max_memory_usage = 100663296, max_bytes_ratio_before_external_group_by = 0;

-- The cap is honoured on evidence that a consumer is draining, so the case where that
-- evidence appears and disappears while a sibling queue stays at the cap needs coverage too:
-- one sparse key and one heavy key interleaved inside the same input blocks, under a
-- sequentially-activated `ConcatProcessor` (produced by narrowing a `UNION ALL` with
-- `max_streams_for_union_step`). This must complete rather than stall: an implementation that
-- tracks the drain evidence per processor rather than per port stalls here in 6 of 6 runs.
DROP TABLE IF EXISTS test_116038_mixed;
CREATE TABLE test_116038_mixed (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0;
-- The sharder is as wide as the streams the reader produces, which can be below `max_threads`
-- and varies with what the statements above left in this database, so the keys are chosen to
-- work at any width rather than at one: key 4 routes to shard 0 - the input the narrowed
-- `ConcatProcessor` demands first - at every width from 4 to 16, while key 0 never does, so the
-- sparse shard is the demanded one while the heavy key fills a sibling queue to the cap. Both
-- keys landing on one shard lets the input finish before that state is reached, and the arm
-- degenerates into a plain query. The narrowing itself is what builds the `Concat`.
INSERT INTO test_116038_mixed SELECT if(number % 100 = 0, 4, 0) AS a, number AS b FROM numbers(2000000)
SETTINGS max_insert_threads = 1;

-- Precondition: both halves of the topology this arm depends on, asserted rather than assumed -
-- a sharder fanning out to more than one output, and the narrowed sequential `ConcatProcessor`
-- that consumes them. Without the narrowing the query still returns the same sums, so the
-- assertion is what keeps the arm from going vacuous. The fan-out is matched by shape rather
-- than by a fixed number because the width follows how many streams the reader produces, which
-- the statements above influence; the keys are chosen to hold at every width this can take.
SELECT countIf(match(explain, 'BufferedShardByHashTransform × \\d+ 1 → ([2-9]|\\d\\d+)')) > 0
       AND countIf(match(explain, 'Concat \\d+ → 1')) > 0
FROM (
    EXPLAIN PIPELINE
    SELECT a, max(s)
    FROM (
        SELECT a, sum(b) AS s FROM test_116038_mixed GROUP BY a
        UNION ALL
        SELECT a, sum(b) AS s FROM test_116038_mixed GROUP BY a
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
    SELECT a, sum(b) AS s FROM test_116038_mixed GROUP BY a
    UNION ALL
    SELECT a, sum(b) AS s FROM test_116038_mixed GROUP BY a
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

DROP TABLE test_116038_mixed;

DROP TABLE test_116038;
