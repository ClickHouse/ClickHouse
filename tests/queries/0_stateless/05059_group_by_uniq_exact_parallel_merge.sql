-- GROUP BY with uniqExact states large enough to be merged in parallel (deferred multi-way merge).
-- One heavily skewed key makes the per-thread partial sets cross the parallel-merge threshold.

-- Two-level result table, partial sets past the two-level threshold themselves.
SELECT k, uniqExact(n) AS u, count() AS c
FROM (SELECT if(number < 4800000, toUInt64(0), number % 16) AS k, number AS n FROM numbers_mt(6000000))
GROUP BY k ORDER BY k
SETTINGS max_threads = 16, group_by_two_level_threshold = 8;

-- Single-level result table, single-level partial sets: the merge converts them to two-level in parallel first.
-- With the hash-partitioned parallel single-level merge (the default) ...
SELECT k, uniqExact(n) AS u
FROM (SELECT if(number < 1200000, toUInt64(0), number % 4) AS k, number AS n FROM numbers_mt(1600000))
GROUP BY k ORDER BY k
SETTINGS max_threads = 16, group_by_two_level_threshold = 0, group_by_two_level_threshold_bytes = 0, enable_parallel_single_level_merge = 1;

-- ... and with the serial single-level merge of the whole tables.
SELECT k, uniqExact(n) AS u
FROM (SELECT if(number < 1200000, toUInt64(0), number % 4) AS k, number AS n FROM numbers_mt(1600000))
GROUP BY k ORDER BY k
SETTINGS max_threads = 16, group_by_two_level_threshold = 0, group_by_two_level_threshold_bytes = 0, enable_parallel_single_level_merge = 0;

-- Variadic uniqExact goes through the same deferred path.
SELECT k, uniqExact(n, s) AS u
FROM (SELECT number % 2 AS k, number AS n, toString(number % 500000) AS s FROM numbers_mt(2000000))
GROUP BY k ORDER BY k
SETTINGS max_threads = 16, group_by_two_level_threshold = 8;

-- Blocks of partial states from remote shards are merged through the same deferred path,
-- with and without memory-efficient (bucket by bucket) merging.
-- Two-level partial states ...
SELECT k, uniqExact(n) AS u
FROM remote('127.0.0.{1,2}', view(SELECT if(number < 1200000, toUInt64(0), number % 4) AS k, number AS n FROM numbers_mt(1600000)))
GROUP BY k ORDER BY k
SETTINGS max_threads = 16, distributed_aggregation_memory_efficient = 0;

SELECT k, uniqExact(n) AS u
FROM remote('127.0.0.{1,2}', view(SELECT if(number < 1200000, toUInt64(0), number % 4) AS k, number AS n FROM numbers_mt(1600000)))
GROUP BY k ORDER BY k
SETTINGS max_threads = 16, distributed_aggregation_memory_efficient = 1, group_by_two_level_threshold = 2;

-- ... and single-level partial states that are converted to two-level in parallel before merging.
SELECT k, uniqExact(n) AS u
FROM remote('127.0.0.{1,2}', view(SELECT if(number < 60000, toUInt64(0), number % 4) AS k, number AS n FROM numbers_mt(80000)))
GROUP BY k ORDER BY k
SETTINGS max_threads = 16, distributed_aggregation_memory_efficient = 0;

-- Partial states spilled to disk are merged back through the block merge path too.
SELECT k, uniqExact(n) AS u, count() AS c
FROM (SELECT if(number < 4800000, toUInt64(0), number % 16) AS k, number AS n FROM numbers_mt(6000000))
GROUP BY k ORDER BY k
SETTINGS max_threads = 16, group_by_two_level_threshold = 8, max_bytes_before_external_group_by = 1, max_bytes_ratio_before_external_group_by = 0;

-- Single thread: nothing is deferred, results must match.
SELECT k, uniqExact(n) AS u
FROM (SELECT if(number < 1200000, toUInt64(0), number % 4) AS k, number AS n FROM numbers_mt(1600000))
GROUP BY k ORDER BY k
SETTINGS max_threads = 1;
