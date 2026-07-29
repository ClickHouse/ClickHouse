-- Tags: no-random-merge-tree-settings
-- ^ The deadlock is purely a pipeline/query-settings phenomenon and is independent of the
--   table's on-disk layout (verified across index_granularity 1/8192 and wide/compact parts).
--   Disabling MergeTree randomization only avoids unrelated CREATE TABLE failures from settings
--   like part_minmax_index_columns that would mask the regression with a BAD_ARGUMENTS error.

-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/106237
-- `BufferedShardByHashTransform` deadlocked the pipeline when a downstream `ConcatProcessor`
-- (here produced by narrowing a `UNION ALL` with `max_streams_for_union_step` < pipeline width)
-- activated its inputs sequentially and the data hashed to only a subset of shards.
-- The empty-queue output ports were never finished, so `Concat` waited forever on an empty
-- branch while the chunks queued on the loaded shards could never drain -> `Pipeline stuck`.
-- The three low-cardinality keys guarantee the skew that triggers the stuck state.

DROP TABLE IF EXISTS test_106237;
CREATE TABLE test_106237 (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO test_106237 SELECT 0 AS a, number AS b FROM numbers(100000);
INSERT INTO test_106237 SELECT 1 AS a, number AS b FROM numbers(100000);
INSERT INTO test_106237 SELECT 2 AS a, number AS b FROM numbers(100000);

-- max_threads must be high enough that the sharded pipeline opens several shard outputs:
-- with the skewed keys above, a wider fan-out guarantees the sequentially-activated
-- ConcatProcessor demands an empty shard, which is what triggers the stuck state. A small
-- value (e.g. 2-3) routes all keys onto the demanded shards and hides the bug.
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
         max_streams_for_union_step = 1;

-- Same deadlock via a scalar-subquery-wrapped UNION ALL (found by the AST-fuzzer oracle,
-- see the issue thread). The stuck state is a property of the sharded transform, not of
-- where the UNION ALL sits, so wrapping it in a scalar subquery reaches the same code path.
SELECT (
    SELECT count() FROM (
        SELECT a, sum(b) AS s FROM test_106237 GROUP BY a
        UNION ALL
        SELECT a, sum(b) AS s FROM test_106237 GROUP BY a
    ) SETTINGS enable_sharding_aggregator = 1,
              max_threads = 16,
              max_streams_for_union_step = 1
);

-- Cover the soft-cap paths in `BufferedShardByHashTransform::prepare`. The two queries
-- above keep every shard queue at 3 chunks or fewer, so `MAX_QUEUE_LENGTH` is never
-- reached and neither cap branch runs. A small `max_block_size` over a larger table
-- makes one sharding transform accumulate hundreds of chunks, which reaches both.

-- (a) Cap reached while a sibling port has an empty queue and downstream demand:
--     the bypass keeps pulling input instead of back-pressuring, which is what the
--     deadlock fix requires. A single key sends every row to one shard.
DROP TABLE IF EXISTS test_106237_cap;
CREATE TABLE test_106237_cap (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO test_106237_cap SELECT 7 AS a, number AS b FROM numbers(1000000);

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
         max_block_size = 100;

-- (b) Cap reached while NO port has an empty queue, so the transform must back-pressure.
--     Distinct keys spread rows over every shard, so no shard queue is ever empty.
DROP TABLE IF EXISTS test_106237_spread;
CREATE TABLE test_106237_spread (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO test_106237_spread SELECT number AS a, number AS b FROM numbers(1000000);

SELECT count(), sum(s)
FROM (SELECT a, sum(b) AS s FROM test_106237_spread GROUP BY a)
SETTINGS enable_sharding_aggregator = 1,
         max_threads = 16,
         max_block_size = 100;

DROP TABLE test_106237_spread;
DROP TABLE test_106237_cap;
DROP TABLE test_106237;
