-- GROUP BY with uniqExact states large enough to be merged in parallel (deferred multi-way merge).
-- One heavily skewed key makes the per-thread partial sets cross the parallel-merge threshold.
-- The sets are kept just past the thresholds: the test is repeated many times by the flaky check.

-- Two-level result table, partial sets past the two-level threshold themselves.
SELECT k, uniqExact(n) AS u, count() AS c
FROM (SELECT if(number < 240000, toUInt64(0), number % 16) AS k, number AS n FROM numbers_mt(256000))
GROUP BY k ORDER BY k
SETTINGS max_threads = 2, max_block_size = 8192, group_by_two_level_threshold = 8;

-- Single-level result table, single-level partial sets: they are merged in parallel without converting them to two-level.
-- With the hash-partitioned parallel single-level merge (the default) ...
SELECT k, uniqExact(n) AS u
FROM (SELECT if(number < 200000, toUInt64(0), number % 4) AS k, number AS n FROM numbers_mt(240000))
GROUP BY k ORDER BY k
SETTINGS max_threads = 4, group_by_two_level_threshold = 0, group_by_two_level_threshold_bytes = 0, enable_parallel_single_level_merge = 1;

-- ... and with the serial single-level merge of the whole tables.
SELECT k, uniqExact(n) AS u
FROM (SELECT if(number < 200000, toUInt64(0), number % 4) AS k, number AS n FROM numbers_mt(240000))
GROUP BY k ORDER BY k
SETTINGS max_threads = 4, group_by_two_level_threshold = 0, group_by_two_level_threshold_bytes = 0, enable_parallel_single_level_merge = 0;

-- Fixed-size keys (UInt8 / UInt16) use the parallel merge of the single-level fixed hash map,
-- whose workers own disjoint subsets of the keys and merge their deferred pairs themselves.
SELECT k, uniqExact(n) AS u
FROM (SELECT if(number < 200000, toUInt8(0), toUInt8(number % 4)) AS k, number AS n FROM numbers_mt(240000))
GROUP BY k ORDER BY k
SETTINGS max_threads = 4;

SELECT k, uniqExact(n) AS u, uniqExactIf(n, n % 3 != 0) AS i
FROM (SELECT if(number < 200000, toUInt16(1000), toUInt16(number % 4)) AS k, number AS n FROM numbers_mt(240000))
GROUP BY k ORDER BY k
SETTINGS max_threads = 4;

-- Variadic uniqExact goes through the same deferred path.
SELECT k, uniqExact(n, s) AS u
FROM (SELECT number % 2 AS k, number AS n, toString(number % 60000) AS s FROM numbers_mt(240000))
GROUP BY k ORDER BY k
SETTINGS max_threads = 4, group_by_two_level_threshold = 8;

-- Blocks of partial states from remote shards are merged through the same deferred path,
-- with and without memory-efficient (bucket by bucket) merging.
-- Two-level partial states ...
SELECT k, uniqExact(n) AS u
FROM remote('127.0.0.{1,2}', view(SELECT if(number < 200000, toUInt64(0), number % 4) AS k, number AS n FROM numbers_mt(240000)))
GROUP BY k ORDER BY k
SETTINGS max_threads = 4, distributed_aggregation_memory_efficient = 0;

SELECT k, uniqExact(n) AS u
FROM remote('127.0.0.{1,2}', view(SELECT if(number < 200000, toUInt64(0), number % 4) AS k, number AS n FROM numbers_mt(240000)))
GROUP BY k ORDER BY k
SETTINGS max_threads = 4, distributed_aggregation_memory_efficient = 1, group_by_two_level_threshold = 2;

-- ... and single-level partial states.
SELECT k, uniqExact(n) AS u
FROM remote('127.0.0.{1,2}', view(SELECT if(number < 60000, toUInt64(0), number % 4) AS k, number AS n FROM numbers_mt(80000)))
GROUP BY k ORDER BY k
SETTINGS max_threads = 4, distributed_aggregation_memory_efficient = 0;

-- Partial states spilled to disk are merged back through the block merge path too.
SELECT k, uniqExact(n) AS u, count() AS c
FROM (SELECT if(number < 240000, toUInt64(0), number % 16) AS k, number AS n FROM numbers_mt(256000))
GROUP BY k ORDER BY k
SETTINGS max_threads = 4, group_by_two_level_threshold = 8, max_bytes_before_external_group_by = 1, max_bytes_ratio_before_external_group_by = 0;

-- Combinators forward the deferred merge to the nested uniqExact: -If, -Tuple and the Nullable adapter,
-- on the keyed merge, the state-block merge from remote shards and the spilled-state merge.
SELECT k, uniqExactIf(n, n % 3 != 0) AS u
FROM (SELECT if(number < 200000, toUInt64(0), number % 4) AS k, number AS n FROM numbers_mt(240000))
GROUP BY k ORDER BY k
SETTINGS max_threads = 4, group_by_two_level_threshold = 8;

SELECT k, uniqExactTuple((n, intDiv(n, 2))) AS u
FROM (SELECT if(number < 200000, toUInt64(0), number % 4) AS k, number AS n FROM numbers_mt(240000))
GROUP BY k ORDER BY k
SETTINGS max_threads = 4, group_by_two_level_threshold = 8;

SELECT k, uniqExact(nullIf(n, 7)) AS u
FROM (SELECT if(number < 200000, toUInt64(0), number % 4) AS k, number AS n FROM numbers_mt(240000))
GROUP BY k ORDER BY k
SETTINGS max_threads = 4, group_by_two_level_threshold = 8;

SELECT k, uniqExactIf(n, n % 3 != 0) AS u, uniqExactTuple((n, intDiv(n, 2))) AS t
FROM remote('127.0.0.{1,2}', view(SELECT if(number < 200000, toUInt64(0), number % 4) AS k, number AS n FROM numbers_mt(240000)))
GROUP BY k ORDER BY k
SETTINGS max_threads = 4, distributed_aggregation_memory_efficient = 0;

SELECT k, uniqExactIf(n, n % 3 != 0) AS u, uniqExactTuple((n, intDiv(n, 2))) AS t
FROM (SELECT if(number < 200000, toUInt64(0), number % 4) AS k, number AS n FROM numbers_mt(240000))
GROUP BY k ORDER BY k
SETTINGS max_threads = 4, group_by_two_level_threshold = 8, max_bytes_before_external_group_by = 1, max_bytes_ratio_before_external_group_by = 0;

-- Single thread: nothing is deferred, results must match.
SELECT k, uniqExact(n) AS u
FROM (SELECT if(number < 200000, toUInt64(0), number % 4) AS k, number AS n FROM numbers_mt(240000))
GROUP BY k ORDER BY k
SETTINGS max_threads = 1;

-- Selective predicate: many partial states of the large key are empty and must be merged as a no-op.
SELECT k, uniqExactIf(n, n < 150000) AS u, uniqExact(if(n < 150000, n, NULL)) AS nu
FROM (SELECT if(number < 200000, toUInt64(0), number % 4) AS k, number AS n FROM numbers_mt(240000))
GROUP BY k ORDER BY k
SETTINGS max_threads = 4, group_by_two_level_threshold = 8;

-- -OrNull and -OrDefault forward the deferred merge too, on the keyed merge, the state-block merge
-- from remote shards and the spilled-state merge.
SELECT k, uniqExactOrNull(n) AS u, uniqExactIfOrDefault(n, n % 3 != 0) AS d
FROM (SELECT if(number < 200000, toUInt64(0), number % 4) AS k, number AS n FROM numbers_mt(240000))
GROUP BY k ORDER BY k
SETTINGS max_threads = 4, group_by_two_level_threshold = 8;

SELECT k, uniqExactOrNull(n) AS u, uniqExactIfOrDefault(n, n % 3 != 0) AS d
FROM remote('127.0.0.{1,2}', view(SELECT if(number < 200000, toUInt64(0), number % 4) AS k, number AS n FROM numbers_mt(240000)))
GROUP BY k ORDER BY k
SETTINGS max_threads = 4, distributed_aggregation_memory_efficient = 0;

SELECT k, uniqExactOrNull(n) AS u, uniqExactIfOrDefault(n, n % 3 != 0) AS d
FROM (SELECT if(number < 200000, toUInt64(0), number % 4) AS k, number AS n FROM numbers_mt(240000))
GROUP BY k ORDER BY k
SETTINGS max_threads = 4, group_by_two_level_threshold = 8, max_bytes_before_external_group_by = 1, max_bytes_ratio_before_external_group_by = 0;

-- Non-memory-efficient merge of two-level blocks from remote shards merges the buckets on a thread pool,
-- and many buckets hold large states whose parallel merge schedules jobs on a thread pool too.
-- This must not deadlock by waiting for nested jobs that can never be scheduled.
SELECT count(), sum(u), max(u)
FROM
(
    SELECT k, uniqExact(n) AS u
    FROM remote('127.0.0.{1,2}', view(SELECT if(number < 480000, intHash64(number % 8), number) AS k, number AS n FROM numbers_mt(485000)))
    GROUP BY k
)
SETTINGS max_threads = 4, distributed_aggregation_memory_efficient = 0, group_by_two_level_threshold = 1, group_by_two_level_threshold_bytes = 1;

-- After `max_rows_to_group_by` is reached the serial single-level merge only merges the keys already in the result,
-- the others into the overflow row (with totals) or nowhere; the large states of the existing keys are deferred too.
-- The first row of every block has the hot key 0, so every thread has it and it always survives the limit.
SELECT k, uniqExact(n) AS u
FROM (SELECT if(number % 8 = 0 AND number % 8192 != 0, number % 64 + 1, 0) AS k, number AS n FROM numbers_mt(320000))
GROUP BY k WITH TOTALS HAVING k = 0 ORDER BY k
SETTINGS max_threads = 4, max_block_size = 8192, group_by_two_level_threshold = 0, group_by_two_level_threshold_bytes = 0, enable_parallel_single_level_merge = 0, max_rows_to_group_by = 4, group_by_overflow_mode = 'any', totals_mode = 'before_having';

SELECT k, uniqExact(n) AS u
FROM (SELECT if(number % 8 = 0 AND number % 8192 != 0, number % 64 + 1, 0) AS k, number AS n FROM numbers_mt(320000))
GROUP BY k HAVING k = 0 ORDER BY k
SETTINGS max_threads = 4, max_block_size = 8192, group_by_two_level_threshold = 0, group_by_two_level_threshold_bytes = 0, enable_parallel_single_level_merge = 0, max_rows_to_group_by = 4, group_by_overflow_mode = 'any';

-- A nullable grouping key keeps its NULL group in a dedicated slot outside the hash table cells.
-- Its large states are deferred like the others: with a dominant NULL group, on the serial single-level merge,
-- the two-level merge and with a `LowCardinality` key.
SELECT k, uniqExact(n) AS u
FROM (SELECT if(number < 200000, NULL, number % 4) AS k, number AS n FROM numbers_mt(240000))
GROUP BY k ORDER BY k
SETTINGS max_threads = 4, group_by_two_level_threshold = 0, group_by_two_level_threshold_bytes = 0, enable_parallel_single_level_merge = 0;

SELECT k, uniqExact(n) AS u
FROM (SELECT if(number < 200000, NULL, number % 4) AS k, number AS n FROM numbers_mt(240000))
GROUP BY k ORDER BY k
SETTINGS max_threads = 4, group_by_two_level_threshold = 8;

SELECT k, uniqExact(n) AS u
FROM (SELECT toLowCardinality(if(number < 200000, NULL, toString(number % 4))) AS k, number AS n FROM numbers_mt(240000))
GROUP BY k ORDER BY k
SETTINGS max_threads = 4, group_by_two_level_threshold = 0, group_by_two_level_threshold_bytes = 0, enable_parallel_single_level_merge = 0;

-- ArgMin / ArgMax forward the deferred merge to the nested uniqExact, restricted to the states with the extremal key.
-- With `n % 2` every thread has the same maximal key, with `intDiv(n, 50000)` the threads have different minimal keys.
SELECT k, uniqExactArgMax(n, n % 2) AS mx, uniqExactArgMin(n, intDiv(n, 50000)) AS mn
FROM (SELECT if(number < 200000, toUInt64(0), number % 4) AS k, number AS n FROM numbers_mt(240000))
GROUP BY k ORDER BY k
SETTINGS max_threads = 4, group_by_two_level_threshold = 8;

SELECT k, uniqExactArgMax(n, n % 2) AS mx, uniqExactArgMin(n, intDiv(n, 50000)) AS mn
FROM (SELECT if(number < 200000, toUInt64(0), number % 4) AS k, number AS n FROM numbers_mt(240000))
GROUP BY k ORDER BY k
SETTINGS max_threads = 4, group_by_two_level_threshold = 0, group_by_two_level_threshold_bytes = 0, enable_parallel_single_level_merge = 0;

SELECT k, uniqExactArgMax(n, n % 2) AS mx, uniqExactArgMin(n, intDiv(n, 50000)) AS mn
FROM (SELECT if(number < 200000, toUInt64(0), number % 4) AS k, number AS n FROM numbers_mt(240000))
GROUP BY k ORDER BY k
SETTINGS max_threads = 4, group_by_two_level_threshold = 8, max_bytes_before_external_group_by = 1, max_bytes_ratio_before_external_group_by = 0;

SELECT k, uniqExactArgMax(n, n % 2) AS mx, uniqExactArgMin(n, intDiv(n, 50000)) AS mn
FROM remote('127.0.0.{1,2}', view(SELECT if(number < 200000, toUInt64(0), number % 4) AS k, number AS n FROM numbers_mt(240000)))
GROUP BY k ORDER BY k
SETTINGS max_threads = 4, distributed_aggregation_memory_efficient = 0;

SELECT uniqExactArgMax(n, n % 2) AS mx, uniqExactArgMin(n, intDiv(n, 50000)) AS mn
FROM (SELECT number AS n FROM numbers_mt(240000))
SETTINGS max_threads = 4;

SELECT k, uniqExactArgMax(n, n % 2) AS mx, uniqExactArgMin(n, intDiv(n, 50000)) AS mn
FROM (SELECT if(number < 200000, toUInt64(0), number % 4) AS k, number AS n FROM numbers_mt(240000))
GROUP BY k ORDER BY k
SETTINGS max_threads = 1;

-- The sources whose key is already worse than the key of the destination are discarded without being deferred.
SELECT k, uniqExactArgMax(n, toFloat64(intDiv(n, 50000))) AS mx, uniqExactArgMin(n, toString(intDiv(n, 50000))) AS mn
FROM (SELECT if(number < 200000, toUInt64(0), number % 4) AS k, number AS n FROM numbers_mt(240000))
GROUP BY k ORDER BY k
SETTINGS max_threads = 4, group_by_two_level_threshold = 8;
