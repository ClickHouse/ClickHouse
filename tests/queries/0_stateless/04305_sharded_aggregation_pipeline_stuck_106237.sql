-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/106237
-- `BufferedShardByHashTransform` must finish empty-queue output ports as soon as the
-- input is exhausted. Otherwise a downstream `ConcatProcessor` that activates inputs
-- sequentially can wait forever on a shard that received no rows in the current chunk,
-- while the chunks queued on the other shards never drain.
--
-- The repro builds a pipeline with the shape
--   `BufferedShardByHashTransform` -> `Resize` -> `ConcatProcessor` -> `AggregatingTransform`
-- by using `enable_sharding_aggregator = 1` plus an outer GROUP BY over a `UNION ALL`
-- with `max_streams_for_union_step = 1` (which forces a `Concat` to narrow the union).
-- The skewed data (only 3 distinct key values across 5 shards) guarantees at least one
-- shard receives no rows from each `BufferedShardByHashTransform`, which is the exact
-- state that triggered the deadlock before the fix.

DROP TABLE IF EXISTS test_106237;
CREATE TABLE test_106237 (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO test_106237 SELECT 0 AS a, number AS b FROM numbers(100000);
INSERT INTO test_106237 SELECT 1 AS a, number AS b FROM numbers(100000);
INSERT INTO test_106237 SELECT 2 AS a, number AS b FROM numbers(100000);

SELECT a, max(s)
FROM (
    SELECT a, sum(b) AS s FROM test_106237 GROUP BY a
    UNION ALL
    SELECT a, sum(b) AS s FROM test_106237 GROUP BY a
)
GROUP BY a
ORDER BY a
SETTINGS enable_sharding_aggregator = 1,
         max_threads = 5,
         max_streams_for_union_step = 1;

DROP TABLE test_106237;
