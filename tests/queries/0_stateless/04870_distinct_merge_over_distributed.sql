-- Tags: shard

-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/111211
-- `DISTINCT` over a `Merge` table whose child is `Distributed` returned a wrong result multiset,
-- not merely a wrong order: `ReadFromMerge` concatenated the per-shard-sorted streams, and
-- `DistinctSortedStreamTransform` only removes adjacent duplicates, so the second shard's run
-- survived and every value came back twice.
--
-- `optimize_distinct_in_order = 1` is what keeps this discriminating: with it 0 the plan
-- hash-dedups the whole stream and returns the right count even when the order is corrupt.
-- The runner randomizes the setting, so it is pinned here.

DROP TABLE IF EXISTS t_distinct;
DROP TABLE IF EXISTS dist_distinct;
DROP TABLE IF EXISTS merge_distinct;

CREATE TABLE t_distinct (s String) ENGINE = MergeTree ORDER BY s;
INSERT INTO t_distinct SELECT toString(number % 21) FROM numbers(100);

CREATE TABLE dist_distinct AS t_distinct
    ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), t_distinct);
CREATE TABLE merge_distinct AS t_distinct ENGINE = Merge(currentDatabase(), '^dist_distinct$');

-- Both shards of the cluster read the same table, so every value is present twice and `DISTINCT`
-- must fold them back to the 21 distinct values. Counting the rows of a subquery keeps the
-- `ORDER BY` in the plan; a bare `SELECT count()` over the query lets the optimizer drop it,
-- which hides the bug. Expected: 21.
SELECT count() FROM (SELECT DISTINCT s FROM merge_distinct ORDER BY s LIMIT 100)
    SETTINGS max_threads = 1, distributed_aggregation_memory_efficient = 0,
        optimize_distinct_in_order = 1;

-- `DISTINCT ON (s)` parses into `LIMIT 1 BY s`, so this arm covers
-- `LimitBySortedStreamTransform`, which also assumes equal keys are adjacent. It is selected on
-- the sort prefix alone, independently of `optimize_distinct_in_order`, so no pin is needed here.
SELECT count() FROM (SELECT DISTINCT ON (s) s FROM merge_distinct ORDER BY s LIMIT 100)
    SETTINGS max_threads = 1, distributed_aggregation_memory_efficient = 0;

-- The same query straight through the `Distributed` table was always correct. Expected: 21.
SELECT count() FROM (SELECT DISTINCT s FROM dist_distinct ORDER BY s LIMIT 100)
    SETTINGS max_threads = 1, distributed_aggregation_memory_efficient = 0,
        optimize_distinct_in_order = 1;

-- `distributed_group_by_no_merge = 2` reads the children one stage higher, where the sort is
-- merge-only because the shards already aggregated and sorted. The three highest keys of the
-- descending order come out of a single shard once the streams are concatenated, so the values
-- are what distinguishes the two behaviours. Expected: 9 9 8.
SELECT s FROM merge_distinct GROUP BY s ORDER BY s DESC LIMIT 3
    SETTINGS max_threads = 1, distributed_group_by_no_merge = 2;

-- `distributed_push_down_limit` defaults to 1, which sends the `LIMIT` to the shards as well.
-- Turning it off keeps the `LIMIT` on the initiator, so the merge-only sort is reached without
-- one. Expected: 9 9 8.
SELECT s FROM merge_distinct GROUP BY s ORDER BY s DESC LIMIT 3
    SETTINGS max_threads = 1, distributed_group_by_no_merge = 2, distributed_push_down_limit = 0;

DROP TABLE merge_distinct;
DROP TABLE dist_distinct;
DROP TABLE t_distinct;
