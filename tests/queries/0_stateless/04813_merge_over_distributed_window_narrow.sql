-- Companion of 04666_merge_over_distributed_order_by. A `Merge` over `Distributed` must skip
-- `narrowPipe` only when the children actually return per-stream sorted data. A window function
-- query is processed on the shards only up to `WithMergeableState` with no remote `ORDER BY`
-- (the initiator does a full sort above the window), so the pipe must still be narrowed - not
-- narrowing it reopens the "too many files and sockets at the same time" problem this
-- optimization guards against.

DROP TABLE IF EXISTS t_low;
DROP TABLE IF EXISTS t_high;
DROP TABLE IF EXISTS dist_low;
DROP TABLE IF EXISTS dist_high;
DROP TABLE IF EXISTS merge_dist;

CREATE TABLE t_low ENGINE = Memory AS SELECT toInt64(number) AS A FROM numbers(100);
CREATE TABLE t_high ENGINE = Memory AS SELECT toInt64(number + 1000) AS A FROM numbers(100);

CREATE TABLE dist_low AS t_low
    ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), t_low);
CREATE TABLE dist_high AS t_high
    ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), t_high);

CREATE TABLE merge_dist AS t_low ENGINE = Merge(currentDatabase(), '^dist_(low|high)$');

-- Two `Distributed` children with two shards each produce four streams. With a stream multiplier
-- of one they must be narrowed down to a single stream, so no transform runs in four copies.
SELECT countIf(explain LIKE '%× 4%')
FROM
(
    EXPLAIN PIPELINE SELECT A, sum(A) OVER (ORDER BY A) FROM merge_dist ORDER BY A LIMIT 5
)
SETTINGS max_threads = 1, max_streams_multiplier_for_merge_tables = 1, distributed_aggregation_memory_efficient = 0;

-- The same holds with `distributed_aggregation_memory_efficient` enabled, which is the default: the
-- bucket order of memory efficient distributed aggregation is only at stake when the query
-- aggregates, and this one does not.
SELECT countIf(explain LIKE '%× 4%')
FROM
(
    EXPLAIN PIPELINE SELECT A, sum(A) OVER (ORDER BY A) FROM merge_dist ORDER BY A LIMIT 5
)
SETTINGS max_threads = 1, max_streams_multiplier_for_merge_tables = 1, distributed_aggregation_memory_efficient = 1;

-- An aggregating query stopping at `WithMergeableState` keeps its four streams, so that memory
-- efficient distributed aggregation can merge the two-level blocks in bucket order.
SELECT countIf(explain LIKE '%GroupingAggregatedTransform 4 → 1%')
FROM
(
    EXPLAIN PIPELINE SELECT A, count() FROM merge_dist GROUP BY A ORDER BY A LIMIT 5
)
SETTINGS max_threads = 1, max_streams_multiplier_for_merge_tables = 1, distributed_aggregation_memory_efficient = 1,
    group_by_two_level_threshold = 1, group_by_two_level_threshold_bytes = 1;

-- The results of the window query are correct regardless of narrowing (every row appears twice
-- because both shards of the cluster read the same underlying table).
SELECT A, sum(A) OVER (ORDER BY A) FROM merge_dist ORDER BY A LIMIT 5
    SETTINGS max_threads = 1, max_streams_multiplier_for_merge_tables = 1, distributed_aggregation_memory_efficient = 0;

-- A plain `ORDER BY` query is sorted on the shards, so it must not be narrowed even with a
-- stream multiplier of one: the step above `ReadFromMerge` merges four sorted streams.
SELECT countIf(explain LIKE '%MergingSortedTransform 4 → 1%')
FROM
(
    EXPLAIN PIPELINE SELECT A FROM merge_dist ORDER BY A DESC LIMIT 5
)
SETTINGS max_threads = 1, max_streams_multiplier_for_merge_tables = 1, distributed_aggregation_memory_efficient = 0;

DROP TABLE merge_dist;
DROP TABLE dist_high;
DROP TABLE dist_low;
DROP TABLE t_high;
DROP TABLE t_low;
