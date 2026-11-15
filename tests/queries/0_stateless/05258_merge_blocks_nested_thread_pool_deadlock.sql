-- The non-memory-efficient merge of two-level blocks from remote shards merges the buckets on a thread pool,
-- and merging two large two-level `uniqExact` states schedules jobs on a thread pool too.
-- This must not deadlock by waiting for nested jobs that can never be scheduled.
SELECT count(), sum(u), max(u)
FROM
(
    SELECT k, uniqExact(n) AS u
    FROM remote('127.0.0.{1,2}', view(SELECT if(number < 1760000, intHash64(number % 16), number) AS k, number AS n FROM numbers_mt(1860000)))
    GROUP BY k
)
SETTINGS max_threads = 2, distributed_aggregation_memory_efficient = 0, group_by_two_level_threshold = 1, group_by_two_level_threshold_bytes = 1;
