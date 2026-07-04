-- Tags: no-random-merge-tree-settings
-- Regression test for a LOGICAL_ERROR: "Join is supported only for pipelines with one output port, got N and M".
-- With query_plan_join_shard_by_pk_ranges enabled, a full_sorting_merge JOIN whose keys are a PK prefix is
-- planned for sharding (PartitionedFinishSorting keeps several per-shard streams on each side). When a PREWHERE
-- prunes one side to a different number of streams than the other, JoinStep falls back from the by-shards
-- pipeline to the plain YShaped pipeline, which used to require exactly one stream per side and aborted the
-- server in debug/sanitizer builds.

DROP TABLE IF EXISTS t04501;
CREATE TABLE t04501 (c0 UInt64, c1 UInt64, s String) ENGINE = MergeTree ORDER BY (c0, c1) SETTINGS index_granularity = 8;
INSERT INTO t04501 SELECT number, number % 10, 'x' FROM numbers(100);
INSERT INTO t04501 SELECT number + 100, number % 10, 'x' FROM numbers(100);
INSERT INTO t04501 SELECT number + 200, number % 10, 'x' FROM numbers(100);

SET enable_analyzer = 1;
SET join_algorithm = 'full_sorting_merge';
SET query_plan_join_shard_by_pk_ranges = 1;
SET max_threads = 8;

-- Two-way self join. `c0 > 1000` is a data-dependent range on the primary key: no part contains such a row,
-- so the left side is fully pruned to a single stream while the sharded right side keeps several streams.
-- That divergence used to abort the server; it must return 0 rows now. `c0 > 1000` is a real column comparison
-- that survives analysis (unlike a constant-foldable predicate such as isNull() on a non-nullable column).
SELECT count() FROM t04501 AS a ALL INNER JOIN t04501 AS b ON b.c0 = a.c0 PREWHERE a.c0 > 1000;

-- Three-way self join with the same primary-key pruning on the outer join's left side: the inner join shards
-- (keeps several streams), the pruned outer-join left arrives with a different stream count than its right.
-- Must not abort the server.
SELECT count() FROM t04501 AS a ALL INNER JOIN t04501 AS b ON b.c0 = a.c0 ALL INNER JOIN t04501 AS c ON b.c1 = c.c1 PREWHERE a.c0 > 1000 WHERE b.c0 != c.c0;

-- Non-empty divergent case: exercise the fallback's row semantics, not only that it stopped throwing.
-- A RIGHT join with `PREWHERE a.c0 > 1000` prunes the left (`a`) side to a single empty stream while the
-- sharded right (`b`) side keeps several non-empty per-shard streams (8 vs 1). The stream counts diverge, so
-- the YShaped fallback runs and merges `b`'s several sorted streams back into one before the merge join.
-- Because it is a RIGHT join, every `b` row reaches the output, so a duplicate, drop or mis-order bug in that
-- merge would change the result. Compare the row count and a row-order-independent checksum of the joined
-- columns against the hash join, which does not use this pipeline. Must be 1.
SELECT
    (SELECT (count(), sum(cityHash64(a.c0, a.c1, b.c0, b.c1))) FROM t04501 AS a ALL RIGHT JOIN t04501 AS b ON b.c0 = a.c0 PREWHERE a.c0 > 1000)
  = (SELECT (count(), sum(cityHash64(a.c0, a.c1, b.c0, b.c1))) FROM t04501 AS a ALL RIGHT JOIN t04501 AS b ON b.c0 = a.c0 PREWHERE a.c0 > 1000 SETTINGS join_algorithm = 'hash');

-- Correctness: a sharded three-way self join with a non-empty result must produce the same count as the hash join.
SELECT
    (SELECT count() FROM t04501 AS a ALL INNER JOIN t04501 AS b ON b.c0 = a.c0 ALL INNER JOIN t04501 AS c ON b.c1 = c.c1 WHERE b.c0 != c.c0)
  = (SELECT count() FROM t04501 AS a ALL INNER JOIN t04501 AS b ON b.c0 = a.c0 ALL INNER JOIN t04501 AS c ON b.c1 = c.c1 WHERE b.c0 != c.c0 SETTINGS join_algorithm = 'hash');

DROP TABLE t04501;
