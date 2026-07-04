-- Tags: no-random-merge-tree-settings
-- Regression test for a LOGICAL_ERROR: "Join is supported only for pipelines with one output port, got N and M".
-- With query_plan_join_shard_by_pk_ranges enabled, a full_sorting_merge JOIN whose keys are a PK prefix is
-- planned for sharding (PartitionedFinishSorting keeps several per-shard streams on each side). When the two
-- sides end up with a different number of streams (e.g. a PREWHERE changes how one side is split into layers),
-- JoinStep falls back from the by-shards pipeline to the plain YShaped pipeline, which used to require exactly
-- one stream per side and aborted the server in debug/sanitizer builds.

DROP TABLE IF EXISTS t04501;
CREATE TABLE t04501 (c0 UInt64, c1 UInt64, s String) ENGINE = MergeTree ORDER BY (c0, c1) SETTINGS index_granularity = 8;
INSERT INTO t04501 SELECT number, number % 10, 'x' FROM numbers(100);
INSERT INTO t04501 SELECT number + 100, number % 10, 'x' FROM numbers(100);
INSERT INTO t04501 SELECT number + 200, number % 10, 'x' FROM numbers(100);

SET enable_analyzer = 1;
SET join_algorithm = 'full_sorting_merge';
SET query_plan_join_shard_by_pk_ranges = 1;
SET max_threads = 8;

-- Two-way self join with an always-false PREWHERE that makes the sides diverge in stream count.
-- Must not abort the server. Returns 0 rows.
SELECT count() FROM t04501 AS a ALL INNER JOIN t04501 AS b ON b.c0 = a.c0 PREWHERE isNull(a.s);

-- Three-way self join: the inner join shards (keeps several streams), the outer join does not, so the outer
-- join's left side arrives with several streams while its right side has one. The result must match the hash join.
SELECT
    (SELECT count() FROM t04501 AS a ALL INNER JOIN t04501 AS b ON b.c0 = a.c0 ALL INNER JOIN t04501 AS c ON b.c1 = c.c1 PREWHERE a.s = 'x' WHERE b.c0 != c.c0)
  = (SELECT count() FROM t04501 AS a ALL INNER JOIN t04501 AS b ON b.c0 = a.c0 ALL INNER JOIN t04501 AS c ON b.c1 = c.c1 PREWHERE a.s = 'x' WHERE b.c0 != c.c0 SETTINGS join_algorithm = 'hash');

DROP TABLE t04501;
