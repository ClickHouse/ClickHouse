-- Memory-bound regression test for the bounded forward-progress escape in
-- `BufferedShardByHashTransform`. With extreme hash-key skew - all rows go to a single
-- shard while a sibling output port has an empty queue and downstream demand - the
-- soft-cap bypass would otherwise buffer the entire input on the receiving shard until
-- upstream depletes. The `MAX_QUEUE_HARD_LIMIT` per-shard cap bounds worst-case
-- buffering at `MAX_QUEUE_HARD_LIMIT * num_shards` chunks, after which the pipeline
-- waits and the eager-finish on `input_finished` unblocks it.
--
-- The query uses a single distinct key value so all rows hash to a single shard; the
-- `UNION ALL` with `max_streams_for_union_step = 1` puts a `ConcatProcessor` downstream
-- of the `Resize` that consumes the 5 shard outputs sequentially.

DROP TABLE IF EXISTS test_106237_skew;
CREATE TABLE test_106237_skew (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY tuple();

-- ~6M rows, all with the same key value -> all route to one shard.
INSERT INTO test_106237_skew SELECT 42 AS a, number AS b FROM numbers(2000000);
INSERT INTO test_106237_skew SELECT 42 AS a, number AS b FROM numbers(2000000);
INSERT INTO test_106237_skew SELECT 42 AS a, number AS b FROM numbers(2000000);

SELECT a, max(s)
FROM (
    SELECT a, sum(b) AS s FROM test_106237_skew GROUP BY a
    UNION ALL
    SELECT a, sum(b) AS s FROM test_106237_skew GROUP BY a
)
GROUP BY a
ORDER BY a
SETTINGS enable_sharding_aggregator = 1,
         max_threads = 5,
         max_streams_for_union_step = 1;

DROP TABLE test_106237_skew;
