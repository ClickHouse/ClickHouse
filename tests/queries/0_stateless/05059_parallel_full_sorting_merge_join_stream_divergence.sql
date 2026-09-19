-- A `parallel_full_sorting_merge` join scatters both sides into a fixed number of shards and pairs the
-- shards of the two sides positionally, so neither side may merge its shards back into one stream.

-- The one-thread side on the left.
SELECT count(), sum(cityHash64(l.number, r.number))
FROM (SELECT number FROM system.numbers LIMIT 10) AS l
INNER JOIN (SELECT number FROM system.numbers LIMIT 10 UNION ALL SELECT number FROM system.numbers LIMIT 10) AS r
    ON l.number = r.number
SETTINGS join_algorithm = 'parallel_full_sorting_merge', max_threads = 4;

-- The one-thread side on the right.
SELECT count(), sum(cityHash64(l.number, r.number))
FROM (SELECT number FROM system.numbers LIMIT 10 UNION ALL SELECT number FROM system.numbers LIMIT 10) AS l
INNER JOIN (SELECT number FROM system.numbers LIMIT 10) AS r
    ON l.number = r.number
SETTINGS join_algorithm = 'parallel_full_sorting_merge', max_threads = 4;

-- The same join without sharding, as the expected answer.
SELECT count(), sum(cityHash64(l.number, r.number))
FROM (SELECT number FROM system.numbers LIMIT 10) AS l
INNER JOIN (SELECT number FROM system.numbers LIMIT 10 UNION ALL SELECT number FROM system.numbers LIMIT 10) AS r
    ON l.number = r.number
SETTINGS join_algorithm = 'hash', max_threads = 4;

-- Both sides keep one sorted stream per shard and the join runs once per shard, so the sharding the
-- optimizer asked for survives into the pipeline.
SELECT
    countIf(explain LIKE '%MergingSortedTransform%') AS merged_back,
    countIf(explain LIKE '%ScatterByPartitionTransform%') AS scattered_sides,
    countIf(match(explain, 'MergeJoinTransform . 4 ')) AS joins_per_shard
FROM (
    EXPLAIN PIPELINE
    SELECT count() FROM (SELECT number FROM system.numbers LIMIT 10) AS l
    INNER JOIN (SELECT number FROM system.numbers LIMIT 10 UNION ALL SELECT number FROM system.numbers LIMIT 10) AS r
        ON l.number = r.number
    SETTINGS join_algorithm = 'parallel_full_sorting_merge', max_threads = 4
);

-- The same, with both sides limited to one thread: the sharding used to be collapsed into a single join.
SELECT
    countIf(explain LIKE '%MergingSortedTransform%') AS merged_back,
    countIf(explain LIKE '%ScatterByPartitionTransform%') AS scattered_sides,
    countIf(match(explain, 'MergeJoinTransform . 4 ')) AS joins_per_shard
FROM (
    EXPLAIN PIPELINE
    SELECT count() FROM (SELECT number FROM system.numbers LIMIT 10) AS l
    INNER JOIN (SELECT number FROM system.numbers LIMIT 10) AS r
        ON l.number = r.number
    SETTINGS join_algorithm = 'parallel_full_sorting_merge', max_threads = 4
);

-- A window-frame partitioned sort has no fixed shard count, so it still merges its streams into one.
SELECT
    countIf(explain LIKE '%MergingSortedTransform%') AS merged_back,
    countIf(explain LIKE '%ScatterByPartitionTransform%') AS scattered_sides
FROM (
    EXPLAIN PIPELINE
    SELECT number, sum(number) OVER (PARTITION BY number % 3 ORDER BY number)
    FROM (SELECT number FROM numbers_mt(100) UNION ALL SELECT number FROM numbers_mt(100))
    SETTINGS max_threads = 1
);
