-- Tags: no-random-merge-tree-settings
-- Regression test for `LOGICAL_ERROR: Join is supported only for pipelines with one output port, got N and M`.
-- With `query_plan_join_shard_by_pk_ranges` enabled, a `full_sorting_merge` `JOIN` whose keys are a PK prefix is
-- planned for sharding (`PartitionedFinishSorting` keeps several per-shard streams on each side). When a
-- `PREWHERE` prunes one side to a different number of streams than the other, `JoinStep` falls back from the
-- by-shards pipeline to the plain `YShaped` pipeline, which used to require exactly one stream per side and
-- aborted the server in debug/sanitizer builds.

DROP TABLE IF EXISTS t04501;
CREATE TABLE t04501 (c0 UInt64, c1 UInt64, s String) ENGINE = MergeTree ORDER BY (c0, c1) SETTINGS index_granularity = 8;
INSERT INTO t04501 SELECT number, number % 10, 'x' FROM numbers(100);
INSERT INTO t04501 SELECT number + 100, number % 10, 'x' FROM numbers(100);
INSERT INTO t04501 SELECT number + 200, number % 10, 'x' FROM numbers(100);

SET enable_analyzer = 1;
SET join_algorithm = 'full_sorting_merge';
SET query_plan_join_shard_by_pk_ranges = 1;
SET max_threads = 8;
-- Sharding is only planned on top of a read-in-order `FinishSorting`, so without this the randomized
-- `optimize_read_in_order = 0` leaves a plain sort, nothing diverges and the test checks nothing.
SET optimize_read_in_order = 1;
-- `optimizeJoinByShards` skips a `ReadFromMergeTree` that reads with parallel replicas, so with them enabled
-- nothing shards, nothing diverges and every assertion below holds vacuously.
SET enable_parallel_replicas = 0;

-- Two-way self join. `c0 > 1000` is a data-dependent range on the primary key: no part contains such a row,
-- so the left side is fully pruned to a single stream while the sharded right side keeps several streams.
-- That divergence used to abort the server; it must return 0 rows now. `c0 > 1000` is a real column comparison
-- that survives analysis (unlike a constant-foldable predicate such as `isNull` on a non-nullable column).
SELECT count() FROM t04501 AS a ALL INNER JOIN t04501 AS b ON b.c0 = a.c0 PREWHERE a.c0 > 1000;

-- Three-way self join with the same primary-key pruning on the outer join's left side: the inner join shards
-- (keeps several streams), the pruned outer-join left arrives with a different stream count than its right.
-- Must not abort the server.
SELECT count() FROM t04501 AS a ALL INNER JOIN t04501 AS b ON b.c0 = a.c0 ALL INNER JOIN t04501 AS c ON b.c1 = c.c1 PREWHERE a.c0 > 1000 WHERE b.c0 != c.c0;

-- Non-empty output through the fallback: a `RIGHT` join with `PREWHERE a.c0 > 1000` prunes the left (`a`)
-- side to a single empty stream while the sharded right (`b`) side keeps several non-empty per-shard streams
-- (8 vs 1). The stream counts diverge, so the fallback merges `b`'s streams back into one, and every merged
-- `b` row reaches the output: a row that merge drops or duplicates changes the count or the checksum. Order
-- is not covered here, because with `a` exhausted the merge join emits the rest of `b` without comparing
-- keys. Compare against the hash join, which does not use this pipeline. Must be 1.
SELECT
    (SELECT (count(), sum(cityHash64(a.c0, a.c1, b.c0, b.c1))) FROM t04501 AS a ALL RIGHT JOIN t04501 AS b ON b.c0 = a.c0 PREWHERE a.c0 > 1000)
  = (SELECT (count(), sum(cityHash64(a.c0, a.c1, b.c0, b.c1))) FROM t04501 AS a ALL RIGHT JOIN t04501 AS b ON b.c0 = a.c0 PREWHERE a.c0 > 1000 SETTINGS join_algorithm = 'hash');

-- The divergence itself only arises when a `PREWHERE` prunes one side to zero rows: with any surviving row
-- both sides stay sharded and equal, so the results above cannot distinguish the sorted merge from an
-- unordered `resize(1)`. Assert the sorted merge in the plan instead. Must be 1.
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT count() FROM t04501 AS a ALL RIGHT JOIN t04501 AS b ON b.c0 = a.c0 PREWHERE a.c0 > 1000
) WHERE explain ILIKE '%MergingSortedTransform 8 %';

-- Correctness: a sharded three-way self join with a non-empty result must produce the same count as the hash join.
SELECT
    (SELECT count() FROM t04501 AS a ALL INNER JOIN t04501 AS b ON b.c0 = a.c0 ALL INNER JOIN t04501 AS c ON b.c1 = c.c1 WHERE b.c0 != c.c0)
  = (SELECT count() FROM t04501 AS a ALL INNER JOIN t04501 AS b ON b.c0 = a.c0 ALL INNER JOIN t04501 AS c ON b.c1 = c.c1 WHERE b.c0 != c.c0 SETTINGS join_algorithm = 'hash');

DROP TABLE t04501;
