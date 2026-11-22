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

-- A result above max_block_size is also split into about one chunk per output stream, not into
-- max_block_size chunks: 3000 groups used to come out as 2001 + 999 rows, now as four chunks of about 750.
SELECT max(bs) < 1000 FROM (SELECT blockSize() AS bs FROM (SELECT number % 3000 AS k, count() FROM numbers_mt(30000) GROUP BY k))
SETTINGS max_threads = 4, max_block_size = 2000, enable_parallel_single_level_merge = 0;

-- The same split applies to each set of GROUPING SETS.
SELECT max(bs) < 20000 FROM (SELECT blockSize() AS bs FROM (SELECT number % 20000 AS k, number % 3 AS g, count() FROM numbers_mt(200000) GROUP BY GROUPING SETS ((k), (g))))
SETTINGS max_threads = 4;

DROP TABLE t_fanout;
