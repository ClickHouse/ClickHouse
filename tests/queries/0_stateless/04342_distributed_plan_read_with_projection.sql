-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.

-- Regression test: a distributed read (make_distributed_plan) over a table with a normal projection
-- used to abort with LOGICAL_ERROR 'Different list of shards in child plans'. The projection
-- optimization replaced the single read with a Union of (surviving-parts read, projection read), but
-- only the surviving-parts branch carried the distributed (sharded) flag, so the two Union branches
-- exposed different shard lists and makeDistributedPlan asserted on the mismatch. The projection
-- optimization now declines for distributed reads, keeping the read whole.

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1 (id UInt32, s String) ENGINE = MergeTree ORDER BY id;
-- Small granularity so the read spans several granules; the projection split must produce a
-- non-trivial read on both Union branches to reach the sharded-read decision (a single-granule read
-- does not reproduce). A few hundred granules is plenty and keeps the fixture cheap.
CREATE TABLE t2 (id1 UInt32, id2 UInt32) ENGINE = MergeTree ORDER BY id1 SETTINGS index_granularity = 16;

-- Two inserts so some parts are served by the projection and some by the surviving parts; the
-- projection ADD between them leaves the first batch's parts without the projection. This mix of
-- projection / non-projection parts is what reproduces the shard-list mismatch, so keep it.
INSERT INTO t2 SELECT number, number % 10 FROM numbers(2000);
ALTER TABLE t2 ADD PROJECTION proj (SELECT id2 ORDER BY id2);
INSERT INTO t2 SELECT number, number % 10 FROM numbers(2000);

INSERT INTO t1 SELECT number, toString(number) FROM numbers(100);

-- Pin max_rows_to_group_by = 0: the outer count() is an AggregatingStep and a nonzero limit (which
-- randomized settings can set) makes make_distributed_plan reject the query before it reaches the
-- projection regression this test targets.
SET max_rows_to_group_by = 0;
-- Pin optimize_use_projections = 1. Without it, randomized settings can disable projection
-- optimization, so optimizeUseNormalProjections never runs, no Union split happens, and neither the
-- fixed nor the unfixed binary aborts, so the test would pass trivially and prove nothing.
SET optimize_use_projections = 1;
-- Pin distributed_plan_max_rows_to_broadcast low so t2's read is sharded (the bug path) without a
-- huge fixture; t1 stays under the threshold and is broadcast, as in the original bug. Otherwise
-- randomized settings could raise it above the selected row count and skip the sharded read.
SET make_distributed_plan = 1, enable_parallel_replicas = 0, distributed_plan_execute_locally = 1,
    distributed_plan_default_shuffle_join_bucket_count = 3, distributed_plan_default_reader_bucket_count = 3,
    distributed_plan_max_rows_to_broadcast = 10;

-- t2's read is sharded; the projection match would split it into a Union. t1 is broadcast.
SELECT '-- distributed read over a projected table does not abort';
SELECT count() FROM (
    SELECT s FROM t1 AS lhs LEFT JOIN (SELECT * FROM t2 PREWHERE id2 = 2 WHERE id2 = 2) AS rhs ON lhs.id = rhs.id2
);

-- Same query single-node, for an explicit value to compare against.
SELECT '-- matches the single-node result';
SELECT count() FROM (
    SELECT s FROM t1 AS lhs LEFT JOIN (SELECT * FROM t2 PREWHERE id2 = 2 WHERE id2 = 2) AS rhs ON lhs.id = rhs.id2
) SETTINGS make_distributed_plan = 0;

DROP TABLE t1;
DROP TABLE t2;
