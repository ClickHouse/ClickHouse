-- A single-level result smaller than max_block_size is split into several chunks when the aggregation
-- output fans out, so the step after it does not run in one thread. 20000 groups stay single-level.
DROP TABLE IF EXISTS t_fanout;
CREATE TABLE t_fanout (n UInt64) ENGINE = MergeTree ORDER BY n AS SELECT number FROM numbers(200000);

-- Several aggregating streams, serial single-level merge.
SELECT max(bs) < 20000 FROM (SELECT blockSize() AS bs FROM (SELECT number % 20000 AS k, count() FROM numbers_mt(200000) GROUP BY k))
SETTINGS max_threads = 4, enable_parallel_single_level_merge = 0;

-- One aggregating stream.
SELECT max(bs) < 20000 FROM (SELECT blockSize() AS bs FROM (SELECT number % 20000 AS k, count() FROM numbers(200000) GROUP BY k))
SETTINGS max_threads = 4;

-- Merge of results from remote shards.
SELECT max(bs) < 20000 FROM (SELECT blockSize() AS bs FROM (SELECT n % 20000 AS k, count() FROM remote('127.0.0.{1,2}', currentDatabase(), t_fanout) GROUP BY k))
SETTINGS max_threads = 4, distributed_aggregation_memory_efficient = 0;

-- Results are unchanged by the split.
SELECT sum(c) = 200000 AND count() = 20000 FROM (SELECT number % 20000 AS k, count() AS c FROM numbers(200000) GROUP BY k) SETTINGS max_threads = 4;

DROP TABLE t_fanout;
