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
